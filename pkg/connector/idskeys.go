// corten-matrix - A Matrix-iMessage puppeting bridge.

package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"github.com/lrhodin/corten-matrix/pkg/rustpushgo"
)

// IDS lookup limiter and status cache.
//
// Every iMessage send, receipt, typing notice and StatusKit resolution needs
// the recipient's IDS identity ("is this handle on iMessage, and what are its
// device keys"). rustpush caches positive answers until Apple's token expiry
// and empty answers for an hour, but caches nothing on error, and Apple
// throttles a busy or new registration by answering "no identities" rather
// than with an error code. Left alone, that turns one throttled hour into a
// storm: every receipt and typing event re-queries, every send to a throttled
// contact is rerouted to the SMS relay, and the extra queries keep the
// account throttled.
//
// This file puts the bridge on the same footing as Apple's own client
// (identityservicesd), whose behaviour on this machine was read out of its
// query cache and server bag:
//
//   - A per-handle status cache in the daemon's own terms — available,
//     unavailable, unknown — served for md-peer-lookup-positive-cache-time
//     (8h), md-peer-lookup-negative-cache-time (24h) and
//     md-peer-lookup-unknown-cache-time (15 min) respectively. Cached keys
//     stay usable for sends until the 7-day hard expiry even once they are
//     past the 8h soft refresh ("last resort cache").
//   - Lookups only for the handles about to be used, batched into one query,
//     with concurrent requests for the same handles coalesced onto one
//     in-flight query instead of duplicated.
//   - A per-hour query budget (device-queries-per-hour) and a single global
//     backoff mode (isInServerBackoffMode) entered when Apple errors or when
//     it starts answering "no identity" for contacts the bridge has already
//     exchanged iMessages with — which is what a throttle looks like on the
//     wire. While backed off, receipts, typing and StatusKit resolution are
//     paused and sends fail visibly instead of falling back to SMS.
//   - No SMS fallback for a recipient the bridge knows is on iMessage (an
//     email handle, or a chat with iMessage history). An empty answer for
//     them is a throttle, not a change on their side, so the message waits
//     rather than going out as a text.
//
// The entry point on the send path is ensureIdentityKeys; the other seams
// are allowOptionalIDS (receipts, typing, delivery receipts),
// validateTargetsSafe (contact_merge.go, which now routes through
// validateTargetsLimited) and the StatusKit resolvers' throttled() checks.

// Apple's identityservicesd cache policy (iMessage server bag defaults) and
// the bridge's own budget on top of it.
const (
	// idsAvailableTTL is how long a positive answer is served without a
	// refresh: md-peer-lookup-positive-cache-time, 28800s. Apple also sends
	// a per-lookup refresh interval with the keys; the shorter one wins.
	idsAvailableTTL = 8 * time.Hour
	// idsUnavailableTTL is how long "not on iMessage" is believed:
	// md-peer-lookup-negative-cache-time, 86400s.
	idsUnavailableTTL = 24 * time.Hour
	// idsUnknownTTL is how long a failed lookup is not retried:
	// md-peer-lookup-unknown-cache-time, 900s.
	idsUnknownTTL = 15 * time.Minute
	// idsHardExpiry is how long cached keys stay usable for a send once past
	// their soft refresh: the dates-expire window is 7 days on every madrid
	// session row on this machine.
	idsHardExpiry = 7 * 24 * time.Hour
	// idsRecentAvailableWindow is how long a past positive answer keeps
	// marking a handle as "known iMessage", so a later empty answer for it
	// is read as a throttle rather than as the contact leaving iMessage.
	idsRecentAvailableWindow = 30 * 24 * time.Hour

	// idsLookupsPerHour is the outbound query budget per rolling hour, all
	// purposes combined. identityservicesd keeps the same counter
	// (device-queries-per-hour); its madrid ceiling on a mature device is
	// above 200/h, but a fresh registration is throttled far sooner, so the
	// bridge stays well under it. A batched query counts once.
	idsLookupsPerHour = 60
	// idsLookupBatch caps handles per query. rustpush chunks at 18; Apple's
	// own max-uri-multi-query is 35.
	idsLookupBatch = 18

	// idsBackoffMin / idsBackoffMax bound the global backoff window. Each
	// consecutive throttle signal doubles it: 15m, 30m, 1h, 2h, 4h, 6h.
	idsBackoffMin = 15 * time.Minute
	idsBackoffMax = 6 * time.Hour
	// idsThrottleStreak is how many consecutive suspicious empty answers
	// (distinct known-iMessage handles) it takes to declare the account
	// throttled and enter backoff.
	idsThrottleStreak = 3
	// idsMissBackoffMax caps the per-handle retry interval after repeated
	// suspicious empty answers: 15m, 30m, 1h, ... up to a day.
	idsMissBackoffMax = 24 * time.Hour
)

// KV keys. Per-handle entries and the global backoff both survive restarts,
// so a crash loop cannot reset the budget Apple has already told us we spent.
const (
	idsStatusKeyPrefix = "ids.status."
	idsBackoffKey      = "ids.backoff"
)

// idsStatus mirrors the IDStatus values identityservicesd stores per handle.
type idsStatus uint8

const (
	idsUnknown     idsStatus = 0
	idsAvailable   idsStatus = 1
	idsUnavailable idsStatus = 2
)

func (s idsStatus) String() string {
	switch s {
	case idsAvailable:
		return "available"
	case idsUnavailable:
		return "unavailable"
	}
	return "unknown"
}

// idsStatusEntry is the cached answer for one handle.
type idsStatusEntry struct {
	Status idsStatus `json:"s"`
	// LookedUpAt is when the answer was obtained (unix ms).
	LookedUpAt int64 `json:"t"`
	// ExpiresAt is when the answer stops being served and a lookup is
	// allowed again (unix ms).
	ExpiresAt int64 `json:"e"`
	// LastAvailableAt is the last time this handle had iMessage keys (unix
	// ms). Proof the contact is on iMessage; used to classify later empties.
	LastAvailableAt int64 `json:"a,omitempty"`
	// Misses counts consecutive suspicious empty answers. Drives the
	// per-handle retry backoff; reset by any positive answer.
	Misses int `json:"m,omitempty"`
}

func (e *idsStatusEntry) fresh(now time.Time) bool {
	return e != nil && now.UnixMilli() < e.ExpiresAt
}

// knownIMessage reports whether the handle has had iMessage keys recently
// enough that an empty answer should be read as throttling.
func (e *idsStatusEntry) knownIMessage(now time.Time) bool {
	return e != nil && e.LastAvailableAt > 0 && now.Sub(time.UnixMilli(e.LastAvailableAt)) < idsRecentAvailableWindow
}

// idsBackoffState is the global backoff mode, persisted under idsBackoffKey.
type idsBackoffState struct {
	Until  int64  `json:"until"`
	Level  int    `json:"level"`
	Reason string `json:"reason,omitempty"`
}

// idsRoute is what the send path learns from ensureIdentityKeys.
type idsRoute struct {
	// noSmsFallback: fail with a visible error rather than resend over SMS
	// when rustpush finds no identity. Set for known-iMessage recipients.
	noSmsFallback bool
	// smsOnly: the recipient is not on iMessage; send as a text message
	// directly instead of paying for a doomed iMessage attempt.
	smsOnly bool
}

// idsPurpose distinguishes a user's message, which is worth a query and a
// visible failure, from the optional traffic that should never spend the
// budget under pressure.
type idsPurpose int

const (
	idsPurposeSend idsPurpose = iota
	idsPurposeOptional
)

// idsLookupLimiter holds the cache, the budget and the backoff for one
// account. Zero value is not usable; IMClient binds it lazily through
// idsLimiter().
type idsLookupLimiter struct {
	mu      sync.Mutex
	entries map[string]*idsStatusEntry
	loaded  map[string]bool
	backoff idsBackoffState
	// backoffLoaded is set once the persisted backoff has been read.
	backoffLoaded bool
	// hourStart / hourCount implement the rolling per-hour budget.
	hourStart time.Time
	hourCount int
	// streak counts consecutive suspicious empties across handles.
	streak int
	// routes holds the decision ensureIdentityKeys made for a portal until
	// HandleMatrixMessage consumes it for the conversation it builds.
	routes map[string]idsRoute
	// inflight maps a handle to the query currently fetching it.
	inflight map[string]*idsInflight
	// sendTimeout / optionalTimeout bound how long ensure waits on a query.
	sendTimeout     time.Duration
	optionalTimeout time.Duration

	// Injected dependencies, so the policy is testable without a bridge.
	now     func() time.Time
	lookup  func(targets []string, allowNetwork, forSend bool) rustpushgo.IdsLookupReport
	kvGet   func(ctx context.Context, key string) string
	kvSet   func(ctx context.Context, key, value string)
	history func(handle string) bool
	log     zerolog.Logger
}

func newIDSLookupLimiter() *idsLookupLimiter {
	return &idsLookupLimiter{
		entries:         make(map[string]*idsStatusEntry),
		loaded:          make(map[string]bool),
		routes:          make(map[string]idsRoute),
		inflight:        make(map[string]*idsInflight),
		sendTimeout:     idsLookupTimeoutSend,
		optionalTimeout: idsLookupTimeoutOptional,
		now:             time.Now,
		kvGet:           func(context.Context, string) string { return "" },
		kvSet:           func(context.Context, string, string) {},
		history:         func(string) bool { return false },
		lookup: func(targets []string, allowNetwork, forSend bool) rustpushgo.IdsLookupReport {
			return rustpushgo.IdsLookupReport{Error: ptrString("no client")}
		},
		log: zerolog.Nop(),
	}
}

func ptrString(s string) *string { return &s }

// idsLimiter returns the account's limiter, binding it to the live client,
// KV store and history check on first use.
func (c *IMClient) idsLimiter() *idsLookupLimiter {
	c.idsLimiterOnce.Do(func() {
		l := newIDSLookupLimiter()
		l.log = c.UserLogin.Log.With().Str("component", "ids-limiter").Logger()
		l.lookup = c.lookupTargetsSafe
		if c.Main != nil && c.Main.Bridge != nil && c.Main.Bridge.DB != nil {
			kv := c.Main.Bridge.DB.KV
			prefix := "login." + string(c.UserLogin.ID) + "."
			l.kvGet = func(ctx context.Context, key string) string { return kv.Get(ctx, database.Key(prefix+key)) }
			l.kvSet = func(ctx context.Context, key, value string) { kv.Set(ctx, database.Key(prefix+key), value) }
		}
		l.history = c.hasIMessageHistory
		c.idsLimiterState = l
	})
	return c.idsLimiterState
}

// lookupTargetsSafe crosses into the identity-manager FFI path, which has
// reachable panic sites upstream; a panic degrades to a lookup error.
func (c *IMClient) lookupTargetsSafe(targets []string, allowNetwork, forSend bool) (report rustpushgo.IdsLookupReport) {
	if c.client == nil {
		return rustpushgo.IdsLookupReport{Error: ptrString("not connected")}
	}
	defer func() {
		if r := recover(); r != nil {
			c.UserLogin.Log.Error().Interface("panic", r).Int("targets", len(targets)).
				Msg("LookupTargets panicked in FFI path")
			report = rustpushgo.IdsLookupReport{Error: ptrString(fmt.Sprintf("lookup panicked: %v", r))}
		}
	}()
	return c.client.LookupTargets(targets, c.handle, allowNetwork, forSend)
}

// hasIMessageHistory reports whether the bridge has an iMessage (non-SMS)
// chat with this handle: a bridged DM portal that is not carrier-flagged.
// Sent messages are recorded in the cloud store too, so a chat the user
// started from Matrix counts once the first message went through.
func (c *IMClient) hasIMessageHistory(handle string) bool {
	if c.Main == nil || c.Main.Bridge == nil || c.UserLogin == nil {
		return false
	}
	if c.isPortalSMS(handle) {
		return false
	}
	ctx := context.Background()
	portal, err := c.Main.Bridge.GetExistingPortalByKey(ctx, networkid.PortalKey{
		ID:       networkid.PortalID(handle),
		Receiver: c.UserLogin.ID,
	})
	if err != nil || portal == nil || portal.MXID == "" {
		return false
	}
	if c.cloudStore != nil {
		if ts, err := c.cloudStore.getNewestMessageTimestamp(ctx, handle); err == nil && ts > 0 {
			return true
		}
	}
	// A room exists for the chat. Without the cloud store to confirm a
	// message, the portal itself is the evidence.
	return true
}

// isIMessageHandle reports whether a portal ID is a single tel:/mailto:
// handle (as opposed to a group, a business chat or something else).
func isIMessageHandle(id string) bool {
	return strings.HasPrefix(id, "tel:") || strings.HasPrefix(id, "mailto:")
}

func isEmailHandle(id string) bool { return strings.HasPrefix(id, "mailto:") }

// ---------------------------------------------------------------------------
// Cache and persistence

func (l *idsLookupLimiter) entry(ctx context.Context, handle string) *idsStatusEntry {
	if e, ok := l.entries[handle]; ok {
		return e
	}
	if !l.loaded[handle] {
		l.loaded[handle] = true
		if raw := l.kvGet(ctx, idsStatusKeyPrefix+handle); raw != "" {
			var e idsStatusEntry
			if err := json.Unmarshal([]byte(raw), &e); err == nil {
				l.entries[handle] = &e
				return &e
			}
		}
	}
	return nil
}

func (l *idsLookupLimiter) store(ctx context.Context, handle string, e *idsStatusEntry) {
	l.entries[handle] = e
	l.loaded[handle] = true
	if raw, err := json.Marshal(e); err == nil {
		l.kvSet(ctx, idsStatusKeyPrefix+handle, string(raw))
	}
}

func (l *idsLookupLimiter) loadBackoff(ctx context.Context) {
	if l.backoffLoaded {
		return
	}
	l.backoffLoaded = true
	if raw := l.kvGet(ctx, idsBackoffKey); raw != "" {
		_ = json.Unmarshal([]byte(raw), &l.backoff)
	}
}

func (l *idsLookupLimiter) saveBackoff(ctx context.Context) {
	if raw, err := json.Marshal(l.backoff); err == nil {
		l.kvSet(ctx, idsBackoffKey, string(raw))
	}
}

// throttledLocked reports whether global backoff is active.
func (l *idsLookupLimiter) throttledLocked(ctx context.Context) bool {
	l.loadBackoff(ctx)
	return l.now().UnixMilli() < l.backoff.Until
}

// backoffUntilLocked returns when the current backoff ends (zero if none).
func (l *idsLookupLimiter) backoffUntilLocked(ctx context.Context) time.Time {
	if !l.throttledLocked(ctx) {
		return time.Time{}
	}
	return time.UnixMilli(l.backoff.Until)
}

// throttled is the cheap check the optional paths (receipts, typing,
// StatusKit) make before spending anything.
func (l *idsLookupLimiter) throttled(ctx context.Context) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.throttledLocked(ctx)
}

// enterBackoffLocked starts or extends global backoff. The level only climbs
// while a previous window is still fresh in memory, so an isolated failure
// hours later starts again from the shortest window.
func (l *idsLookupLimiter) enterBackoffLocked(ctx context.Context, reason string) time.Time {
	l.loadBackoff(ctx)
	now := l.now()
	level := l.backoff.Level
	if l.backoff.Until > 0 && now.Sub(time.UnixMilli(l.backoff.Until)) > idsBackoffMax {
		level = 0
	}
	d := idsBackoffMin << uint(level)
	if d > idsBackoffMax || d <= 0 {
		d = idsBackoffMax
	}
	until := now.Add(d)
	if until.UnixMilli() > l.backoff.Until {
		l.backoff.Until = until.UnixMilli()
	}
	l.backoff.Level = level + 1
	l.backoff.Reason = reason
	l.streak = 0
	l.saveBackoff(ctx)
	l.log.Warn().
		Str("reason", reason).
		Dur("backoff", d).
		Time("until", time.UnixMilli(l.backoff.Until)).
		Int("level", l.backoff.Level).
		Msg("IDS lookups paused: entering backoff (receipts, typing and StatusKit resolution wait; sends to known-iMessage contacts will not fall back to SMS)")
	return time.UnixMilli(l.backoff.Until)
}

// clearBackoffLocked is called on a positive answer once the window has
// passed: Apple is answering again, so the next incident starts small.
func (l *idsLookupLimiter) clearBackoffLocked(ctx context.Context) {
	l.loadBackoff(ctx)
	if l.backoff.Level == 0 && l.backoff.Until == 0 {
		return
	}
	if l.now().UnixMilli() >= l.backoff.Until {
		l.backoff = idsBackoffState{}
		l.saveBackoff(ctx)
	}
}

// hourUsedLocked rolls the hourly window if needed and returns the queries
// spent in the current one.
func (l *idsLookupLimiter) hourUsedLocked() int {
	now := l.now()
	if l.hourStart.IsZero() || now.Sub(l.hourStart) >= time.Hour {
		l.hourStart = now
		l.hourCount = 0
	}
	return l.hourCount
}

// reserveQueryLocked spends one unit of the hourly budget, or reports that
// it is exhausted.
func (l *idsLookupLimiter) reserveQueryLocked() (ok bool, resetAt time.Time) {
	if l.hourUsedLocked() >= idsLookupsPerHour {
		return false, l.hourStart.Add(time.Hour)
	}
	l.hourCount++
	return true, time.Time{}
}

// ---------------------------------------------------------------------------
// Policy

// Lookup timeouts. A lookup that is still running when its deadline passes
// keeps running in the background and records its answer when it finishes;
// the caller just stops waiting for it. Sends wait as long as rustpush's own
// per-attempt timeout; optional traffic barely waits, since it is skipped
// under any pressure anyway.
const (
	idsLookupTimeoutSend     = 30 * time.Second
	idsLookupTimeoutOptional = 5 * time.Second
)

// idsInflight is a query that has left the process. Callers for the same
// handles wait on it instead of issuing their own (Apple's "piggybacking
// onto in-flight query"); a query older than the send timeout is stalled
// and is neither waited for nor duplicated.
type idsInflight struct {
	done    chan struct{}
	started time.Time
}

// idsHandleState is the per-handle view after the cache has been consulted.
type idsHandleState struct {
	handle string
	entry  *idsStatusEntry
	// usable: rustpush still holds keys inside the hard expiry.
	usable bool
}

// idsPass is one cache pass over a set of targets.
type idsPass struct {
	dec    idsDecision
	states map[string]*idsHandleState
	// waiting: handles inside a miss/failure window; no query for them.
	waiting []string
	// toQuery: handles with no fresh answer and no query in flight.
	toQuery []string
	// stalled: handles whose in-flight query has outlived the send timeout.
	stalled []string
	// inflight: queries covering some targets, to wait for.
	inflight []*idsInflight
}

// syncFromRust folds rustpush's own cache into the Go entry, so a handle
// whose keys arrived through an inbound message (rustpush fetches the
// sender's key to decrypt) is known here without ever having been queried
// by the limiter. Call with l.mu held.
func (l *idsLookupLimiter) syncFromRust(ctx context.Context, now time.Time, o rustpushgo.IdsLookupOutcome) *idsHandleState {
	st := &idsHandleState{handle: o.Handle, entry: l.entry(ctx, o.Handle), usable: o.Usable}
	e := st.entry
	// A fresh miss entry (a send rustpush refused, or an empty answer for a
	// known contact) outranks rustpush's view: its cache can hold keys that
	// lack the device the message came from, and re-trying costs a query.
	inMissBackoff := e != nil && e.Status == idsUnknown && e.Misses > 0 && e.fresh(now)
	switch {
	case o.Status == uint8(idsAvailable) && !inMissBackoff:
		// rustpush has fresh keys. Believe it, and remember the proof.
		if e == nil || e.Status != idsAvailable || !e.fresh(now) {
			ttl := idsAvailableTTL
			if o.RefreshSecs > 0 && time.Duration(o.RefreshSecs)*time.Second < ttl {
				ttl = time.Duration(o.RefreshSecs) * time.Second
			}
			e = &idsStatusEntry{
				Status:          idsAvailable,
				LookedUpAt:      now.UnixMilli(),
				ExpiresAt:       now.Add(ttl).UnixMilli(),
				LastAvailableAt: now.UnixMilli(),
			}
			l.store(ctx, o.Handle, e)
			st.entry = e
		}
	case o.Usable && e != nil && e.LastAvailableAt == 0:
		// Stale-but-usable keys are still proof of iMessage.
		e.LastAvailableAt = now.UnixMilli()
		l.store(ctx, o.Handle, e)
	}
	return st
}

// classifyEmpty decides what a fresh empty answer for handle means.
func (l *idsLookupLimiter) classifyEmpty(now time.Time, handle string, e *idsStatusEntry) (suspicious bool) {
	if e.knownIMessage(now) {
		return true
	}
	return l.history(handle)
}

// idsDecision is the outcome of ensure for a set of targets.
type idsDecision struct {
	// proceed: the send may go ahead (possibly on stale keys).
	proceed bool
	// err is the visible failure when proceed is false.
	err error
	// route carries the SMS routing flags for the send.
	route idsRoute
	// unavailable lists targets known not to be on iMessage.
	unavailable []string
	// available lists targets with keys (fresh or usable).
	available []string
}

// classify is one pass over the cache for targets. It never queries and
// never blocks on the network: probe is the cache-only rustpush view the
// caller fetched beforehand.
func (l *idsLookupLimiter) classify(ctx context.Context, targets []string, probe rustpushgo.IdsLookupReport) idsPass {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	pass := idsPass{dec: idsDecision{proceed: true}, states: make(map[string]*idsHandleState, len(targets))}
	for _, o := range probe.Outcomes {
		pass.states[o.Handle] = l.syncFromRust(ctx, now, o)
	}
	for _, t := range targets {
		if _, ok := pass.states[t]; !ok {
			pass.states[t] = &idsHandleState{handle: t, entry: l.entry(ctx, t)}
		}
	}
	seenInflight := map[*idsInflight]bool{}
	for _, t := range targets {
		st := pass.states[t]
		e := st.entry
		switch {
		case e.fresh(now) && e.Status == idsAvailable:
			pass.dec.available = append(pass.dec.available, t)
		case e.fresh(now) && e.Status == idsUnavailable:
			pass.dec.unavailable = append(pass.dec.unavailable, t)
		case e.fresh(now) && e.Status == idsUnknown:
			// A recent failure still inside its retry interval. Keys rustpush
			// holds carry a send only when the failure was a lookup error
			// (Misses == 0); after a refused send they are the problem.
			if st.usable && e.Misses == 0 {
				pass.dec.available = append(pass.dec.available, t)
			} else {
				pass.waiting = append(pass.waiting, t)
			}
		default:
			if f, ok := l.inflight[t]; ok {
				if time.Since(f.started) > idsLookupTimeoutSend {
					pass.stalled = append(pass.stalled, t)
				} else if !seenInflight[f] {
					seenInflight[f] = true
					pass.inflight = append(pass.inflight, f)
				}
			} else {
				pass.toQuery = append(pass.toQuery, t)
			}
		}
	}
	if len(targets) == 1 {
		t := targets[0]
		pass.dec.route.noSmsFallback = isEmailHandle(t) || pass.states[t].entry.knownIMessage(now) || l.history(t)
	}
	return pass
}

// startLookup spends budget for a query over handles and marks them in
// flight, or explains why no query may leave the process right now.
func (l *idsLookupLimiter) startLookup(ctx context.Context, handles []string, purpose idsPurpose) (f *idsInflight, blockedWhy string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.throttledLocked(ctx) {
		until := l.backoffUntilLocked(ctx)
		return nil, fmt.Sprintf("iMessage lookups are paused until %s because Apple is rate-limiting this account", until.Local().Format("15:04"))
	}
	// Optional traffic may use at most half the hourly budget, so receipts
	// and typing can never starve the user's own messages of lookups.
	if purpose == idsPurposeOptional && l.hourUsedLocked() >= idsLookupsPerHour/2 {
		return nil, "optional share of the hourly IDS lookup budget spent"
	}
	if ok, resetAt := l.reserveQueryLocked(); !ok {
		return nil, fmt.Sprintf("the hourly iMessage lookup budget is spent; lookups resume at %s", resetAt.Local().Format("15:04"))
	}
	f = &idsInflight{done: make(chan struct{}), started: time.Now()}
	for _, h := range handles {
		l.inflight[h] = f
	}
	return f, ""
}

// runLookup performs the query and records its answer. It runs in its own
// goroutine so a stalled Apple never holds any lock or any caller.
func (l *idsLookupLimiter) runLookup(handles []string, forSend bool, f *idsInflight) {
	defer close(f.done)
	l.log.Debug().Strs("handles", handles).Bool("for_send", forSend).Msg("IDS lookup")
	report := l.lookup(handles, true, forSend)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.applyReport(context.Background(), handles, report)
	for _, h := range handles {
		if l.inflight[h] == f {
			delete(l.inflight, h)
		}
	}
}

// applyReport records what Apple answered for handles. Call with l.mu held.
func (l *idsLookupLimiter) applyReport(ctx context.Context, handles []string, report rustpushgo.IdsLookupReport) {
	now := l.now()
	if report.Error != nil {
		// Apple did not answer. Nothing is cached rust-side, so without this
		// the next send would ask again immediately.
		for _, t := range handles {
			e := l.entry(ctx, t)
			if e == nil {
				e = &idsStatusEntry{}
			}
			e.Status = idsUnknown
			e.LookedUpAt = now.UnixMilli()
			e.ExpiresAt = now.Add(idsUnknownTTL).UnixMilli()
			l.store(ctx, t, e)
		}
		reason := fmt.Sprintf("identity lookup failed (%s)", shortIDSError(*report.Error, report.ErrorCode))
		if report.ErrorCode == 6009 {
			reason = "Apple has disabled iMessage for this account (6009)"
		}
		l.enterBackoffLocked(ctx, reason)
		return
	}
	outcomes := make(map[string]rustpushgo.IdsLookupOutcome, len(report.Outcomes))
	for _, o := range report.Outcomes {
		outcomes[o.Handle] = o
	}
	var throttledHandles []string
	sawPositive := false
	for _, t := range handles {
		o, ok := outcomes[t]
		e := l.entry(ctx, t)
		if e == nil {
			e = &idsStatusEntry{}
		}
		switch {
		case ok && o.Status == uint8(idsAvailable):
			ttl := idsAvailableTTL
			if o.RefreshSecs > 0 && time.Duration(o.RefreshSecs)*time.Second < ttl {
				ttl = time.Duration(o.RefreshSecs) * time.Second
			}
			e.Status = idsAvailable
			e.LookedUpAt = now.UnixMilli()
			e.ExpiresAt = now.Add(ttl).UnixMilli()
			e.LastAvailableAt = now.UnixMilli()
			e.Misses = 0
			sawPositive = true
		case ok && o.Status == uint8(idsUnavailable):
			if l.classifyEmpty(now, t, e) {
				// A contact we have exchanged iMessages with "has no
				// identity". That is what Apple's throttle looks like.
				e.Misses++
				d := idsBackoffMin << uint(e.Misses-1)
				if d > idsMissBackoffMax || d <= 0 {
					d = idsMissBackoffMax
				}
				e.Status = idsUnknown
				e.LookedUpAt = now.UnixMilli()
				e.ExpiresAt = now.Add(d).UnixMilli()
				throttledHandles = append(throttledHandles, t)
			} else {
				e.Status = idsUnavailable
				e.LookedUpAt = now.UnixMilli()
				e.ExpiresAt = now.Add(idsUnavailableTTL).UnixMilli()
				e.Misses = 0
			}
		default:
			// Not answered (quarantined, or missing from the report).
			e.Status = idsUnknown
			e.LookedUpAt = now.UnixMilli()
			e.ExpiresAt = now.Add(idsUnknownTTL).UnixMilli()
		}
		l.store(ctx, t, e)
	}
	if sawPositive {
		l.streak = 0
		l.clearBackoffLocked(ctx)
	}
	if len(throttledHandles) > 0 {
		l.streak += len(throttledHandles)
		l.log.Warn().
			Strs("handles", throttledHandles).
			Int("streak", l.streak).
			Msg("IDS answered 'no identity' for known-iMessage contacts; treating as a lookup throttle")
		if l.streak >= idsThrottleStreak && !l.throttledLocked(ctx) {
			l.enterBackoffLocked(ctx, fmt.Sprintf("%d consecutive empty answers for known-iMessage contacts", l.streak))
		}
	}
}

// ensure is the core policy: make sure the targets have identities, query
// Apple when allowed, and say what the caller should do. It never holds a
// lock while waiting on Apple, and never waits longer than the purpose's
// timeout.
func (l *idsLookupLimiter) ensure(ctx context.Context, targets []string, purpose idsPurpose) idsDecision {
	targets = dedupeStrings(targets)
	if len(targets) == 0 {
		return idsDecision{proceed: true}
	}
	forSend := purpose == idsPurposeSend
	timeout := l.sendTimeout
	if !forSend {
		timeout = l.optionalTimeout
	}

	// Pass 1: cache only (Go entries folded with rustpush's cache).
	pass := l.classify(ctx, targets, l.lookup(targets, false, forSend))
	if len(pass.toQuery) == 0 && len(pass.inflight) == 0 {
		if len(pass.stalled) > 0 {
			return l.blocked(pass, purpose, "an iMessage identity lookup for this recipient is still waiting on Apple")
		}
		return l.finish(pass, targets, purpose)
	}

	waitFor := pass.inflight
	if len(pass.toQuery) > 0 {
		toQuery := pass.toQuery
		if len(toQuery) > idsLookupBatch {
			toQuery = toQuery[:idsLookupBatch]
		}
		sort.Strings(toQuery)
		f, why := l.startLookup(ctx, toQuery, purpose)
		if f == nil {
			return l.blocked(pass, purpose, why)
		}
		go l.runLookup(toQuery, forSend, f)
		waitFor = append(waitFor, f)
	}

	// Wait, bounded. Whatever is still running keeps running.
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	timedOut := false
wait:
	for _, f := range waitFor {
		select {
		case <-f.done:
		case <-ctx.Done():
			timedOut = true
			break wait
		case <-timer.C:
			timedOut = true
			break wait
		}
	}

	// Pass 2: read what was recorded.
	pass = l.classify(ctx, targets, l.lookup(targets, false, forSend))
	if timedOut && (len(pass.toQuery) > 0 || len(pass.inflight) > 0 || len(pass.stalled) > 0) {
		l.log.Warn().Strs("handles", targets).Dur("waited", timeout).Msg("IDS lookup still running; not waiting any longer")
		return l.blocked(pass, purpose, "the iMessage identity lookup is taking too long; try again shortly")
	}
	if len(pass.toQuery) > 0 || len(pass.inflight) > 0 || len(pass.stalled) > 0 {
		return l.blocked(pass, purpose, "an iMessage identity lookup for this recipient is still waiting on Apple")
	}
	l.mu.Lock()
	throttled := l.throttledLocked(ctx)
	until := l.backoffUntilLocked(ctx)
	reason := l.backoff.Reason
	l.mu.Unlock()
	if throttled && len(pass.dec.available) == 0 && len(pass.dec.unavailable) == 0 {
		return l.blocked(pass, purpose, fmt.Sprintf("Apple is rate-limiting iMessage lookups for this account (%s); paused until %s", reason, until.Local().Format("15:04")))
	}
	return l.finish(pass, targets, purpose)
}

// finish turns the classified targets into the caller's decision.
func (l *idsLookupLimiter) finish(pass idsPass, targets []string, purpose idsPurpose) idsDecision {
	dec := pass.dec
	if len(targets) != 1 {
		// Groups: rustpush delivers to whichever members have keys. Only a
		// group with no reachable member at all is a failure, and rustpush
		// reports that itself.
		if purpose == idsPurposeOptional && len(dec.available) == 0 {
			dec.proceed = false
			dec.err = errors.New("no group member has a cached iMessage identity")
		}
		return dec
	}
	t := pass.states[targets[0]]
	switch {
	case len(dec.available) > 0:
		return dec
	case len(dec.unavailable) > 0:
		if isEmailHandle(t.handle) {
			dec.proceed = false
			dec.err = idsSendError(fmt.Sprintf("%s is not registered with iMessage", stripIdentifierPrefix(t.handle)))
			return dec
		}
		dec.route.smsOnly = true
		if purpose == idsPurposeOptional {
			dec.proceed = false
			dec.err = errors.New("recipient is not on iMessage")
		}
		return dec
	case len(pass.waiting) > 0:
		retry := time.UnixMilli(t.entry.ExpiresAt)
		dec.proceed = false
		if purpose == idsPurposeOptional {
			dec.err = errors.New("recipient identity is in lookup backoff")
			return dec
		}
		if dec.route.noSmsFallback {
			dec.err = idsSendError(fmt.Sprintf("Apple returned no iMessage identity for %s, which this bridge has messaged over iMessage before; treating it as a lookup rate limit rather than sending as a text. Try again after %s.",
				stripIdentifierPrefix(t.handle), retry.Local().Format("15:04")))
		} else {
			dec.err = idsSendError(fmt.Sprintf("no iMessage identity for %s right now; try again after %s", stripIdentifierPrefix(t.handle), retry.Local().Format("15:04")))
		}
		return dec
	}
	return dec
}

// blocked is the decision when no answer can be obtained right now.
func (l *idsLookupLimiter) blocked(pass idsPass, purpose idsPurpose, why string) idsDecision {
	dec := pass.dec
	// Stale keys still carry a send (Apple's last-resort cache), unless the
	// handle is in miss backoff, where those keys are what failed.
	have := make(map[string]bool, len(dec.available))
	for _, t := range dec.available {
		have[t] = true
	}
	for h, st := range pass.states {
		if !have[h] && st.usable && (st.entry == nil || st.entry.Misses == 0) {
			dec.available = append(dec.available, h)
		}
	}
	if len(dec.available) > 0 {
		return dec
	}
	dec.proceed = false
	if purpose == idsPurposeOptional {
		dec.err = errors.New(why)
	} else {
		dec.err = idsSendError("can't deliver right now: " + why)
	}
	return dec
}

func shortIDSError(msg string, code uint64) string {
	if code != 0 {
		return fmt.Sprintf("IDS %d", code)
	}
	if i := strings.IndexByte(msg, ':'); i > 0 && i < 60 {
		return msg[:i]
	}
	if len(msg) > 80 {
		return msg[:80]
	}
	return msg
}

// idsSendError builds the visible send failure, in the shape errNoCarrierRoute
// uses: status event with the message, plus an in-room notice.
func idsSendError(msg string) error {
	return bridgev2.WrapErrorInStatus(errors.New(msg)).
		WithIsCertain(true).
		WithErrorAsMessage().
		WithSendNotice(true).
		WithErrorReason(event.MessageStatusNetworkError)
}

func dedupeStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// observeAvailable records proof that a handle is on iMessage without a
// lookup: an inbound iMessage from it. Apple's daemon fills its cache the
// same way (query reason URIDecrypt), and rustpush already fetched the key.
func (l *idsLookupLimiter) observeAvailable(ctx context.Context, handle string) {
	if !isIMessageHandle(handle) {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	e := l.entry(ctx, handle)
	if e != nil && e.Status == idsAvailable && e.fresh(now) {
		return
	}
	l.store(ctx, handle, &idsStatusEntry{
		Status:          idsAvailable,
		LookedUpAt:      now.UnixMilli(),
		ExpiresAt:       now.Add(idsAvailableTTL).UnixMilli(),
		LastAvailableAt: now.UnixMilli(),
	})
	l.streak = 0
}

// ---------------------------------------------------------------------------
// IMClient seams

// idsSendTargets lists the recipients a send to this portal will need
// identities for: the handle of a DM, the members of a group (minus our own
// handles). Non-handle portals (business chats) yield nothing.
func (c *IMClient) idsSendTargets(portal *bridgev2.Portal) []string {
	portalID := stripSmsSuffix(string(portal.ID))
	if strings.HasPrefix(portalID, "gid:") || strings.Contains(portalID, ",") {
		var participants []string
		if strings.HasPrefix(portalID, "gid:") {
			if c.cloudStore != nil {
				if parts, err := c.cloudStore.getChatParticipantsByPortalID(context.Background(), string(portal.ID)); err == nil {
					participants = parts
				}
			}
		} else {
			participants = strings.Split(portalID, ",")
		}
		var out []string
		for _, p := range participants {
			p = stripSmsSuffix(p)
			if isIMessageHandle(p) && !c.isMyHandle(p) {
				out = append(out, p)
			}
		}
		return out
	}
	if !isIMessageHandle(portalID) || c.isMyHandle(portalID) {
		return nil
	}
	return []string{portalID}
}

// ensureIdentityKeys runs the outbound delivery-identity precheck for a
// Matrix→iMessage message before it is handed to the sender. A non-nil
// response or error means the send was resolved at this stage and must not
// continue; (nil, nil) lets the normal send path proceed.
//
// SMS chats are left alone: their routing goes through checkCarrierRoute,
// which has its own ceiling.
func (c *IMClient) ensureIdentityKeys(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	if c.client == nil || msg == nil || msg.Portal == nil {
		return nil, nil
	}
	portalID := string(msg.Portal.ID)
	if c.isPortalSMS(portalID) {
		return nil, nil
	}
	targets := c.idsSendTargets(msg.Portal)
	if len(targets) == 0 {
		return nil, nil
	}
	l := c.idsLimiter()
	dec := l.ensure(ctx, targets, idsPurposeSend)
	l.mu.Lock()
	l.routes[portalID] = dec.route
	l.mu.Unlock()
	if !dec.proceed {
		c.UserLogin.Log.Warn().Err(dec.err).Str("portal_id", portalID).Strs("targets", targets).
			Msg("Outbound iMessage held by the IDS lookup limiter")
		return nil, dec.err
	}
	return nil, nil
}

// applyIdentityRouting carries ensureIdentityKeys' decision into the
// conversation HandleMatrixMessage builds: no SMS fallback for a recipient
// known to be on iMessage, and a direct text message for one known not to be.
func (c *IMClient) applyIdentityRouting(portal *bridgev2.Portal, conv *rustpushgo.WrappedConversation) {
	if c.idsLimiterState == nil {
		return
	}
	l := c.idsLimiterState
	l.mu.Lock()
	route, ok := l.routes[string(portal.ID)]
	delete(l.routes, string(portal.ID))
	l.mu.Unlock()
	if !ok || conv.IsSms {
		return
	}
	conv.NoSmsFallback = route.noSmsFallback
	if route.smsOnly {
		conv.IsSms = true
	}
}

// allowOptionalIDS gates the traffic that is nice to have but never worth a
// throttled account: read receipts, typing, delivery receipts. It never
// fails a send and only ever queries within the budget; while backed off it
// answers false immediately.
func (c *IMClient) allowOptionalIDS(ctx context.Context, participants []string) bool {
	if c.client == nil {
		return false
	}
	var targets []string
	for _, p := range participants {
		if isIMessageHandle(p) && !c.isMyHandle(p) {
			targets = append(targets, p)
		}
	}
	if len(targets) == 0 {
		return true
	}
	l := c.idsLimiter()
	if l.throttled(ctx) {
		return false
	}
	dec := l.ensure(ctx, targets, idsPurposeOptional)
	if !dec.proceed {
		c.UserLogin.Log.Debug().Err(dec.err).Strs("targets", targets).Msg("Optional iMessage traffic skipped by the IDS lookup limiter")
	}
	return dec.proceed
}

// validateTargetsLimited is ValidateTargets behind the limiter: it returns
// the targets known or found to be on iMessage, serving the cache first and
// querying only within the budget. Under backoff it answers from the cache
// alone, so a throttled account never mistakes silence for "not on
// iMessage" (which would reroute a message to an alternate number).
func (c *IMClient) validateTargetsLimited(ctx context.Context, targets []string) []string {
	if c.client == nil || len(targets) == 0 {
		return nil
	}
	dec := c.idsLimiter().ensure(ctx, targets, idsPurposeSend)
	return dec.available
}

// idsThrottledForStatusKit is the StatusKit resolvers' check: their lookups
// are the most optional traffic there is, so they pause for the whole window.
func (c *IMClient) idsThrottledForStatusKit(ctx context.Context) bool {
	if c.client == nil {
		return true
	}
	return c.idsLimiter().throttled(ctx)
}

// isIDSIdentityFailure reports whether a rustpush send error means "no
// usable iMessage identity for the recipient": the NoValidTargets variant,
// or the same condition surfacing as text from a receipt or typing send.
func isIDSIdentityFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, rustpushgo.ErrWrappedErrorNoValidTargets) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "does not have iMessage or you are being rate-limited") ||
		strings.Contains(msg, "NoValidTargets") ||
		strings.Contains(msg, "IDS key missing")
}

// recordIdentityFailure notes that rustpush could not deliver to these
// participants for lack of an identity even though the limiter let the send
// through (typically: cached keys that lack the device the message came
// from, which rustpush then re-fetches on every attempt). Each participant
// enters the per-handle miss backoff so the next optional send to it is
// skipped rather than retried, and known-iMessage contacts count toward the
// account-level throttle streak. Returns the affected handles.
func (l *idsLookupLimiter) recordIdentityFailure(ctx context.Context, participants []string, isMine func(string) bool) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	var who []string
	suspicious := 0
	for _, p := range participants {
		if !isIMessageHandle(p) || isMine(p) {
			continue
		}
		who = append(who, p)
		e := l.entry(ctx, p)
		if e == nil {
			e = &idsStatusEntry{}
		}
		if e.Status == idsUnknown && e.Misses > 0 && e.fresh(now) {
			continue // already waiting out a miss
		}
		known := e.knownIMessage(now) || e.Status == idsAvailable || l.history(p)
		e.Misses++
		d := idsBackoffMin << uint(e.Misses-1)
		if d > idsMissBackoffMax || d <= 0 {
			d = idsMissBackoffMax
		}
		e.Status = idsUnknown
		e.LookedUpAt = now.UnixMilli()
		e.ExpiresAt = now.Add(d).UnixMilli()
		l.store(ctx, p, e)
		if known {
			suspicious++
		}
	}
	if suspicious > 0 {
		l.streak += suspicious
		l.log.Warn().Strs("handles", who).Int("streak", l.streak).
			Msg("Send refused for lack of an iMessage identity for known-iMessage contact(s); holding further traffic to them")
		if l.streak >= idsThrottleStreak && !l.throttledLocked(ctx) {
			l.enterBackoffLocked(ctx, fmt.Sprintf("%d consecutive identity failures for known-iMessage contacts", l.streak))
		}
	}
	return who
}

// idsRecordSendFailure feeds a failed receipt/typing/message send back into
// the limiter when the failure was an identity failure. Returns true when it
// was, so callers can drop the error as handled.
func (c *IMClient) idsRecordSendFailure(ctx context.Context, conv rustpushgo.WrappedConversation, err error) bool {
	if !isIDSIdentityFailure(err) || conv.IsSms {
		return false
	}
	c.idsLimiter().recordIdentityFailure(ctx, conv.Participants, c.isMyHandle)
	return true
}

// idsNoIdentityError explains a message send that rustpush refused for lack
// of an iMessage identity after the limiter had ruled out the SMS fallback,
// and records the miss so the next attempt waits instead of querying again.
func (c *IMClient) idsNoIdentityError(ctx context.Context, conv rustpushgo.WrappedConversation) error {
	who := c.idsLimiter().recordIdentityFailure(ctx, conv.Participants, c.isMyHandle)
	for i := range who {
		who[i] = stripIdentifierPrefix(who[i])
	}
	return idsSendError(fmt.Sprintf("Apple returned no iMessage identity for %s, which this bridge has messaged over iMessage before; treating it as a lookup rate limit rather than sending as a text. Try again in a few minutes.", strings.Join(who, ", ")))
}
