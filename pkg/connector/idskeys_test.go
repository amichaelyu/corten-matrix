// corten-matrix - A Matrix-iMessage puppeting bridge.

package connector

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"

	"github.com/lrhodin/corten-matrix/pkg/rustpushgo"
)

// fakeIDS stands in for rustpush: it answers cache probes from `cached` and
// network queries from `remote`, counting the queries that leave the process.
type fakeIDS struct {
	mu      sync.Mutex
	cached  map[string]rustpushgo.IdsLookupOutcome
	remote  map[string]rustpushgo.IdsLookupOutcome
	err     *string
	errCode uint64
	queries int32
	delay   time.Duration
	kv      map[string]string
}

func newFakeIDS() *fakeIDS {
	return &fakeIDS{
		cached: map[string]rustpushgo.IdsLookupOutcome{},
		remote: map[string]rustpushgo.IdsLookupOutcome{},
		kv:     map[string]string{},
	}
}

func (f *fakeIDS) lookup(targets []string, allowNetwork, forSend bool) rustpushgo.IdsLookupReport {
	f.mu.Lock()
	defer f.mu.Unlock()
	var rep rustpushgo.IdsLookupReport
	if allowNetwork {
		atomic.AddInt32(&f.queries, 1)
		rep.Queried = true
		if f.delay > 0 {
			f.mu.Unlock()
			time.Sleep(f.delay)
			f.mu.Lock()
		}
		if f.err != nil {
			rep.Error = f.err
			rep.ErrorCode = f.errCode
		} else {
			// A query populates the cache the way rustpush's put_keys does.
			for _, t := range targets {
				if o, ok := f.remote[t]; ok {
					f.cached[t] = o
				} else {
					f.cached[t] = rustpushgo.IdsLookupOutcome{Handle: t, Status: uint8(idsUnavailable)}
				}
			}
		}
	}
	for _, t := range targets {
		if o, ok := f.cached[t]; ok {
			o.Handle = t
			rep.Outcomes = append(rep.Outcomes, o)
		} else {
			rep.Outcomes = append(rep.Outcomes, rustpushgo.IdsLookupOutcome{Handle: t})
		}
	}
	return rep
}

func available(h string) rustpushgo.IdsLookupOutcome {
	return rustpushgo.IdsLookupOutcome{Handle: h, Status: uint8(idsAvailable), Usable: true, RefreshSecs: 28800}
}

type limiterHarness struct {
	l     *idsLookupLimiter
	ids   *fakeIDS
	clock time.Time
	hist  map[string]bool
}

func newLimiterHarness() *limiterHarness {
	h := &limiterHarness{ids: newFakeIDS(), clock: time.Date(2026, 9, 29, 1, 0, 0, 0, time.UTC), hist: map[string]bool{}}
	h.l = newIDSLookupLimiter()
	h.l.now = func() time.Time { return h.clock }
	h.l.lookup = h.ids.lookup
	h.l.kvGet = func(_ context.Context, k string) string { return h.ids.kv[k] }
	h.l.kvSet = func(_ context.Context, k, v string) { h.ids.kv[k] = v }
	h.l.history = func(handle string) bool { return h.hist[handle] }
	return h
}

func (h *limiterHarness) advance(d time.Duration) { h.clock = h.clock.Add(d) }

func (h *limiterHarness) queries() int { return int(atomic.LoadInt32(&h.ids.queries)) }

const (
	telA  = "tel:+15550000001"
	telB  = "tel:+15550000002"
	telC  = "tel:+15550000003"
	mailA = "mailto:someone@example.com"
)

func TestIDSLimiterServesFreshPositiveWithoutQuery(t *testing.T) {
	h := newLimiterHarness()
	h.ids.remote[telA] = available(telA)
	ctx := context.Background()

	dec := h.l.ensure(ctx, []string{telA}, idsPurposeSend)
	if !dec.proceed || len(dec.available) != 1 || h.queries() != 1 {
		t.Fatalf("first send: %+v queries=%d", dec, h.queries())
	}
	// Within the 8h positive TTL nothing else leaves the process.
	h.advance(7 * time.Hour)
	dec = h.l.ensure(ctx, []string{telA}, idsPurposeSend)
	if !dec.proceed || h.queries() != 1 {
		t.Fatalf("cached send: %+v queries=%d", dec, h.queries())
	}
	if !h.l.ensure(ctx, []string{telA}, idsPurposeOptional).proceed || h.queries() != 1 {
		t.Fatalf("optional traffic should ride the cache")
	}
	// Past the TTL the next send refreshes (one query), like Apple's
	// "status 1, needed difference 0" re-query on next use. rustpush's own
	// entry is past its soft refresh by then too: stale but usable.
	h.advance(2 * time.Hour)
	h.ids.cached[telA] = rustpushgo.IdsLookupOutcome{Handle: telA, Status: uint8(idsUnknown), Usable: true}
	dec = h.l.ensure(ctx, []string{telA}, idsPurposeSend)
	if !dec.proceed || h.queries() != 2 {
		t.Fatalf("refresh: %+v queries=%d", dec, h.queries())
	}
}

func TestIDSLimiterTrustsRustCache(t *testing.T) {
	h := newLimiterHarness()
	// rustpush already holds fresh keys (fetched to decrypt an inbound
	// message); the limiter must not query again.
	h.ids.cached[telA] = available(telA)
	dec := h.l.ensure(context.Background(), []string{telA}, idsPurposeSend)
	if !dec.proceed || h.queries() != 0 {
		t.Fatalf("rust cache hit should not query: %+v queries=%d", dec, h.queries())
	}
	if !dec.route.noSmsFallback {
		t.Fatalf("a handle with keys is known-iMessage: no SMS fallback")
	}
}

func TestIDSLimiterUnavailableTelBecomesSMS(t *testing.T) {
	h := newLimiterHarness()
	ctx := context.Background()
	dec := h.l.ensure(ctx, []string{telB}, idsPurposeSend)
	if !dec.proceed || !dec.route.smsOnly || dec.route.noSmsFallback {
		t.Fatalf("unknown tel with no history and no identity should go as SMS: %+v", dec)
	}
	if h.queries() != 1 {
		t.Fatalf("queries=%d", h.queries())
	}
	// Believed for 24h without another query.
	h.advance(23 * time.Hour)
	dec = h.l.ensure(ctx, []string{telB}, idsPurposeSend)
	if !dec.proceed || !dec.route.smsOnly || h.queries() != 1 {
		t.Fatalf("negative cache: %+v queries=%d", dec, h.queries())
	}
	// Optional traffic to a non-iMessage contact is pointless.
	if h.l.ensure(ctx, []string{telB}, idsPurposeOptional).proceed {
		t.Fatalf("optional traffic to an SMS-only contact should be skipped")
	}
	h.advance(2 * time.Hour)
	h.l.ensure(ctx, []string{telB}, idsPurposeSend)
	if h.queries() != 2 {
		t.Fatalf("after 24h a re-query is due, queries=%d", h.queries())
	}
}

func TestIDSLimiterUnavailableEmailFails(t *testing.T) {
	h := newLimiterHarness()
	dec := h.l.ensure(context.Background(), []string{mailA}, idsPurposeSend)
	if dec.proceed || dec.err == nil || !strings.Contains(dec.err.Error(), "not registered") {
		t.Fatalf("email with no identity must fail, never SMS: %+v", dec)
	}
	if !dec.route.noSmsFallback {
		t.Fatalf("email handles never fall back to SMS")
	}
}

func TestIDSLimiterEmptyForKnownContactIsAThrottle(t *testing.T) {
	h := newLimiterHarness()
	ctx := context.Background()
	h.hist[telA] = true

	dec := h.l.ensure(ctx, []string{telA}, idsPurposeSend)
	if dec.proceed || dec.err == nil {
		t.Fatalf("empty answer for a contact with iMessage history must hold the send: %+v", dec)
	}
	if !strings.Contains(dec.err.Error(), "rate limit") || dec.route.smsOnly || !dec.route.noSmsFallback {
		t.Fatalf("expected a rate-limit explanation and no SMS routing: err=%v route=%+v", dec.err, dec.route)
	}
	if h.l.throttled(ctx) {
		t.Fatalf("one suspicious empty is not yet an account throttle")
	}
	// The miss is remembered: a retry inside the 15m window does not query.
	h.advance(5 * time.Minute)
	dec = h.l.ensure(ctx, []string{telA}, idsPurposeSend)
	if dec.proceed || h.queries() != 1 {
		t.Fatalf("retry inside miss backoff should not query: %+v queries=%d", dec, h.queries())
	}
	// After the window, one more query; the second miss doubles the wait.
	h.advance(11 * time.Minute)
	dec = h.l.ensure(ctx, []string{telA}, idsPurposeSend)
	if dec.proceed || h.queries() != 2 {
		t.Fatalf("second attempt: %+v queries=%d", dec, h.queries())
	}
	e := h.l.entries[telA]
	if e.Misses != 2 || time.UnixMilli(e.ExpiresAt).Sub(h.clock) != 30*time.Minute {
		t.Fatalf("miss backoff should double: %+v", e)
	}
}

func TestIDSLimiterStreakEntersBackoffAndPausesOptional(t *testing.T) {
	h := newLimiterHarness()
	ctx := context.Background()
	for _, tel := range []string{telA, telB, telC} {
		h.hist[tel] = true
	}
	h.l.ensure(ctx, []string{telA}, idsPurposeSend)
	h.l.ensure(ctx, []string{telB}, idsPurposeSend)
	if h.l.throttled(ctx) {
		t.Fatalf("two misses should not throttle yet")
	}
	dec := h.l.ensure(ctx, []string{telC}, idsPurposeSend)
	if dec.proceed || !h.l.throttled(ctx) {
		t.Fatalf("third suspicious empty should enter backoff: %+v", dec)
	}
	if !strings.Contains(dec.err.Error(), "rate-limiting") {
		t.Fatalf("err=%v", dec.err)
	}
	before := h.queries()
	// While backed off, a brand-new handle is not queried and optional
	// traffic is refused outright.
	dec = h.l.ensure(ctx, []string{"tel:+15550000009"}, idsPurposeSend)
	if dec.proceed || h.queries() != before {
		t.Fatalf("send during backoff should be held without a query: %+v queries=%d", dec, h.queries())
	}
	if h.l.ensure(ctx, []string{"tel:+15550000009"}, idsPurposeOptional).proceed {
		t.Fatalf("optional traffic must pause during backoff")
	}
	// Stale keys still carry a send during backoff (last-resort cache), for
	// a contact that has not itself been refused.
	const telD = "tel:+15550000004"
	h.ids.cached[telD] = rustpushgo.IdsLookupOutcome{Handle: telD, Status: uint8(idsUnknown), Usable: true}
	if !h.l.ensure(ctx, []string{telD}, idsPurposeSend).proceed {
		t.Fatalf("usable stale keys should let a send through during backoff")
	}
	// Not for one in miss backoff: rustpush's keys are what failed.
	h.ids.cached[telA] = rustpushgo.IdsLookupOutcome{Handle: telA, Status: uint8(idsUnknown), Usable: true}
	if h.l.ensure(ctx, []string{telA}, idsPurposeSend).proceed {
		t.Fatalf("a refused contact's stale keys must not carry a send")
	}
	// The window is 15m at level 1; afterwards lookups resume, and a
	// positive answer clears the level.
	h.advance(16 * time.Minute)
	if h.l.throttled(ctx) {
		t.Fatalf("backoff should have expired")
	}
	h.ids.remote[telB] = available(telB)
	dec = h.l.ensure(ctx, []string{telB}, idsPurposeSend)
	if !dec.proceed {
		t.Fatalf("after backoff a positive answer proceeds: %+v", dec)
	}
	if h.l.backoff.Level != 0 {
		t.Fatalf("positive answer after the window should clear the backoff level, got %d", h.l.backoff.Level)
	}
}

func TestIDSLimiterBackoffDoublesWhileHot(t *testing.T) {
	h := newLimiterHarness()
	ctx := context.Background()
	h.l.enterBackoffLocked(ctx, "test")
	first := time.UnixMilli(h.l.backoff.Until).Sub(h.clock)
	h.advance(first + time.Minute)
	h.l.enterBackoffLocked(ctx, "test")
	second := time.UnixMilli(h.l.backoff.Until).Sub(h.clock)
	if first != idsBackoffMin || second != 2*idsBackoffMin {
		t.Fatalf("expected %v then %v, got %v then %v", idsBackoffMin, 2*idsBackoffMin, first, second)
	}
	// Level climbs to the cap and no further.
	for i := 0; i < 10; i++ {
		h.advance(time.UnixMilli(h.l.backoff.Until).Sub(h.clock) + time.Minute)
		h.l.enterBackoffLocked(ctx, "test")
	}
	if d := time.UnixMilli(h.l.backoff.Until).Sub(h.clock); d != idsBackoffMax {
		t.Fatalf("capped backoff = %v", d)
	}
	// A failure long after the last window starts small again.
	h.advance(idsBackoffMax * 3)
	h.l.enterBackoffLocked(ctx, "test")
	if d := time.UnixMilli(h.l.backoff.Until).Sub(h.clock); d != idsBackoffMin {
		t.Fatalf("cold backoff should restart at %v, got %v", idsBackoffMin, d)
	}
}

func TestIDSLimiterLookupErrorBacksOff(t *testing.T) {
	h := newLimiterHarness()
	ctx := context.Background()
	msg := "Bad authentication, try again and re-enter device details if persistent. (6005)"
	h.ids.err = &msg
	h.ids.errCode = 6005
	dec := h.l.ensure(ctx, []string{telA}, idsPurposeSend)
	if dec.proceed || !strings.Contains(dec.err.Error(), "6005") || !h.l.throttled(ctx) {
		t.Fatalf("lookup error should hold the send and enter backoff: %+v", dec)
	}
	if e := h.l.entries[telA]; e == nil || e.Status != idsUnknown || time.UnixMilli(e.ExpiresAt).Sub(h.clock) != idsUnknownTTL {
		t.Fatalf("failed lookup should be cached as unknown for %v: %+v", idsUnknownTTL, e)
	}
	if h.l.ensure(ctx, []string{telA}, idsPurposeSend).proceed || h.queries() != 1 {
		t.Fatalf("no re-query while the unknown entry is fresh, queries=%d", h.queries())
	}
}

func TestIDSLimiterHourlyBudget(t *testing.T) {
	h := newLimiterHarness()
	ctx := context.Background()
	for i := 0; i < idsLookupsPerHour; i++ {
		tel := "tel:+1555100" + string(rune('0'+i%10)) + string(rune('0'+(i/10)%10)) + "00"
		h.ids.remote[tel] = available(tel)
		if !h.l.ensure(ctx, []string{tel}, idsPurposeSend).proceed {
			t.Fatalf("send %d should proceed", i)
		}
	}
	if h.queries() != idsLookupsPerHour {
		t.Fatalf("queries=%d", h.queries())
	}
	dec := h.l.ensure(ctx, []string{telC}, idsPurposeSend)
	if dec.proceed || !strings.Contains(dec.err.Error(), "budget") || h.queries() != idsLookupsPerHour {
		t.Fatalf("budget exhausted: %+v queries=%d", dec, h.queries())
	}
	if h.l.throttled(ctx) {
		t.Fatalf("a spent budget is not a backoff")
	}
	h.advance(time.Hour)
	h.ids.remote[telC] = available(telC)
	if !h.l.ensure(ctx, []string{telC}, idsPurposeSend).proceed {
		t.Fatalf("budget should reset after an hour")
	}
}

func TestIDSLimiterOptionalGetsHalfTheBudget(t *testing.T) {
	h := newLimiterHarness()
	ctx := context.Background()
	half := idsLookupsPerHour / 2
	for i := 0; i < half; i++ {
		tel := "tel:+1555200" + string(rune('0'+i%10)) + string(rune('0'+(i/10)%10)) + "00"
		h.ids.remote[tel] = available(tel)
		h.l.ensure(ctx, []string{tel}, idsPurposeOptional)
	}
	if h.queries() != half {
		t.Fatalf("queries=%d", h.queries())
	}
	h.ids.remote[telA] = available(telA)
	if h.l.ensure(ctx, []string{telA}, idsPurposeOptional).proceed || h.queries() != half {
		t.Fatalf("optional traffic past its half share must not query")
	}
	if !h.l.ensure(ctx, []string{telA}, idsPurposeSend).proceed || h.queries() != half+1 {
		t.Fatalf("a send still has budget left")
	}
}

func TestIDSLimiterPersistsAcrossRestart(t *testing.T) {
	h := newLimiterHarness()
	ctx := context.Background()
	h.ids.remote[telA] = available(telA)
	h.l.ensure(ctx, []string{telA}, idsPurposeSend)
	h.hist[telB], h.hist[telC] = true, true
	h.l.ensure(ctx, []string{telB}, idsPurposeSend)
	h.l.ensure(ctx, []string{telC}, idsPurposeSend)
	h.l.ensure(ctx, []string{"tel:+15550000004"}, idsPurposeSend) // unavailable, no history
	h.l.enterBackoffLocked(ctx, "restart test")

	// A fresh limiter on the same KV (a restarted bridge) knows everything.
	h2 := newLimiterHarness()
	h2.ids.kv = h.ids.kv
	h2.clock = h.clock
	h2.hist = h.hist
	if !h2.l.throttled(ctx) {
		t.Fatalf("backoff must survive a restart")
	}
	if e := h2.l.entry(ctx, telA); e == nil || e.Status != idsAvailable {
		t.Fatalf("positive entry not restored: %+v", e)
	}
	if e := h2.l.entry(ctx, telB); e == nil || e.Misses != 1 {
		t.Fatalf("miss entry not restored: %+v", e)
	}
	if e := h2.l.entry(ctx, "tel:+15550000004"); e == nil || e.Status != idsUnavailable {
		t.Fatalf("negative entry not restored: %+v", e)
	}
	if h2.queries() != 0 {
		t.Fatalf("restoring must not query")
	}
}

func TestIDSLimiterObserveInbound(t *testing.T) {
	h := newLimiterHarness()
	ctx := context.Background()
	h.l.observeAvailable(ctx, telA)
	dec := h.l.ensure(ctx, []string{telA}, idsPurposeSend)
	if !dec.proceed || h.queries() != 0 || !dec.route.noSmsFallback {
		t.Fatalf("an inbound message is proof of iMessage: %+v queries=%d", dec, h.queries())
	}
	// Later, an empty answer for that contact is a throttle, not SMS.
	h.advance(9 * time.Hour)
	h.ids.cached = map[string]rustpushgo.IdsLookupOutcome{}
	dec = h.l.ensure(ctx, []string{telA}, idsPurposeSend)
	if dec.proceed || dec.route.smsOnly {
		t.Fatalf("known contact with empty answer must not be texted: %+v", dec)
	}
	h.l.observeAvailable(ctx, "gid:not-a-handle")
	if _, ok := h.l.entries["gid:not-a-handle"]; ok {
		t.Fatalf("only tel:/mailto: handles are tracked")
	}
}

func TestIDSLimiterCoalescesConcurrentLookups(t *testing.T) {
	h := newLimiterHarness()
	h.ids.remote[telA] = available(telA)
	h.ids.delay = 30 * time.Millisecond
	var wg sync.WaitGroup
	var ok int32
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if h.l.ensure(context.Background(), []string{telA}, idsPurposeSend).proceed {
				atomic.AddInt32(&ok, 1)
			}
		}()
	}
	wg.Wait()
	if ok != 5 || h.queries() != 1 {
		t.Fatalf("five concurrent sends should share one query: ok=%d queries=%d", ok, h.queries())
	}
}

func TestIDSLimiterGroupsUseWhoeverHasKeys(t *testing.T) {
	h := newLimiterHarness()
	ctx := context.Background()
	h.ids.remote[telA] = available(telA)
	dec := h.l.ensure(ctx, []string{telA, telB}, idsPurposeSend)
	if !dec.proceed || len(dec.available) != 1 || len(dec.unavailable) != 1 || dec.route.smsOnly {
		t.Fatalf("group: %+v", dec)
	}
	if h.queries() != 1 {
		t.Fatalf("one batched query for the group, got %d", h.queries())
	}
	// Optional traffic to a group with no reachable member is skipped.
	if h.l.ensure(ctx, []string{telC}, idsPurposeOptional).proceed {
		t.Fatalf("no member with keys")
	}
}

func TestIDSSendErrorIsVisible(t *testing.T) {
	err := idsSendError("held")
	var status bridgev2.MessageStatus
	if !errors.As(err, &status) {
		// WrapErrorInStatus returns a MessageStatus value; make sure the
		// shape matches errNoCarrierRoute so clients render the text.
		t.Fatalf("idsSendError should be a bridgev2.MessageStatus: %T", err)
	}
	if !status.SendNotice || !status.IsCertain || !status.ErrorAsMessage || status.InternalError == nil {
		t.Fatalf("status not user-visible: %+v", status)
	}
}

func TestShortIDSError(t *testing.T) {
	if got := shortIDSError("anything", 6005); got != "IDS 6005" {
		t.Fatalf("got %q", got)
	}
	if got := shortIDSError("guarded lookup budget exhausted: details", 0); got != "guarded lookup budget exhausted" {
		t.Fatalf("got %q", got)
	}
}

func TestIDSLimiterRecordsSendFailuresBehindRustCache(t *testing.T) {
	h := newLimiterHarness()
	ctx := context.Background()
	// rustpush holds keys for the sender, so the limiter lets a receipt
	// through; rustpush then refuses it (the device token is not among the
	// cached keys, and the refresh came back empty).
	h.ids.cached[telA] = available(telA)
	if !h.l.ensure(ctx, []string{telA}, idsPurposeOptional).proceed {
		t.Fatalf("receipt should be allowed on cached keys")
	}
	err := errors.New("WrappedError: GenericError: Msg=Failed to send delivery receipt: Could not deliver message. The recipient does not have iMessage or you are being rate-limited.")
	if !isIDSIdentityFailure(err) {
		t.Fatalf("rustpush's NoValidTargets text should classify as an identity failure")
	}
	if isIDSIdentityFailure(errors.New("Send timeout; try again")) {
		t.Fatalf("a timeout is not an identity failure")
	}
	who := h.l.recordIdentityFailure(ctx, []string{"tel:+15550009999", telA}, func(h string) bool { return h == "tel:+15550009999" })
	if len(who) != 1 || who[0] != telA {
		t.Fatalf("own handle must be excluded: %v", who)
	}
	// The next receipt to that sender is held without a query, even though
	// rustpush still claims fresh keys.
	dec := h.l.ensure(ctx, []string{telA}, idsPurposeOptional)
	if dec.proceed || h.queries() != 0 {
		t.Fatalf("held: %+v queries=%d", dec, h.queries())
	}
	if e := h.l.entries[telA]; e.Misses != 1 || e.Status != idsUnknown {
		t.Fatalf("entry=%+v", e)
	}
	// Repeated failures for the same handle inside the window do not stack.
	h.l.recordIdentityFailure(ctx, []string{telA}, func(string) bool { return false })
	if h.l.entries[telA].Misses != 1 || h.l.streak != 1 {
		t.Fatalf("stacked: misses=%d streak=%d", h.l.entries[telA].Misses, h.l.streak)
	}
	// Three known contacts refused in a row → account-level backoff.
	h.ids.cached[telB], h.ids.cached[telC] = available(telB), available(telC)
	h.l.ensure(ctx, []string{telB}, idsPurposeOptional)
	h.l.ensure(ctx, []string{telC}, idsPurposeOptional)
	h.l.recordIdentityFailure(ctx, []string{telB}, func(string) bool { return false })
	h.l.recordIdentityFailure(ctx, []string{telC}, func(string) bool { return false })
	if !h.l.throttled(ctx) {
		t.Fatalf("three refused known contacts should enter backoff")
	}
	// A user's message to the held contact still gets a visible hold, not SMS.
	dec = h.l.ensure(ctx, []string{telA}, idsPurposeSend)
	if dec.proceed || dec.route.smsOnly || !dec.route.noSmsFallback {
		t.Fatalf("send to held contact: %+v", dec)
	}
	// After the miss window, and once the backoff has passed, a positive
	// answer restores the handle.
	h.advance(idsBackoffMin + time.Minute)
	dec = h.l.ensure(ctx, []string{telA}, idsPurposeSend)
	if !dec.proceed {
		t.Fatalf("after the window, rust's fresh keys are believed again: %+v", dec)
	}
}

func TestIDSLimiterDoesNotBlockOnASlowLookup(t *testing.T) {
	h := newLimiterHarness()
	ctx := context.Background()
	h.l.sendTimeout = 50 * time.Millisecond
	h.l.optionalTimeout = 10 * time.Millisecond
	h.ids.remote[telA] = available(telA)
	h.ids.delay = 300 * time.Millisecond

	started := time.Now()
	dec := h.l.ensure(ctx, []string{telA}, idsPurposeSend)
	if waited := time.Since(started); waited > 200*time.Millisecond {
		t.Fatalf("send waited %v on a stalled lookup", waited)
	}
	if dec.proceed || dec.err == nil || !strings.Contains(dec.err.Error(), "taking too long") {
		t.Fatalf("slow lookup should fail visibly, not hang: %+v", dec)
	}
	// While that query is still out, other callers neither wait for it nor
	// duplicate it, and a different handle is not held hostage by it.
	started = time.Now()
	if h.l.ensure(ctx, []string{telA}, idsPurposeOptional).proceed {
		t.Fatalf("optional traffic should skip while the lookup is out")
	}
	if waited := time.Since(started); waited > 100*time.Millisecond {
		t.Fatalf("optional waited %v", waited)
	}
	if h.queries() != 1 {
		t.Fatalf("queries=%d, the stalled query must not have been duplicated", h.queries())
	}
	// Once the background query lands, its answer is served from cache.
	time.Sleep(400 * time.Millisecond)
	dec = h.l.ensure(ctx, []string{telA}, idsPurposeSend)
	if !dec.proceed || h.queries() != 1 {
		t.Fatalf("background answer not recorded: %+v queries=%d", dec, h.queries())
	}
}

func TestIDSLimiterWaitersShareOneInflightQuery(t *testing.T) {
	h := newLimiterHarness()
	h.ids.remote[telA] = available(telA)
	h.ids.delay = 80 * time.Millisecond
	var wg sync.WaitGroup
	var ok int32
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if h.l.ensure(context.Background(), []string{telA}, idsPurposeSend).proceed {
				atomic.AddInt32(&ok, 1)
			}
		}()
		time.Sleep(5 * time.Millisecond)
	}
	wg.Wait()
	if ok != 4 || h.queries() != 1 {
		t.Fatalf("late callers should wait on the in-flight query: ok=%d queries=%d", ok, h.queries())
	}
}
