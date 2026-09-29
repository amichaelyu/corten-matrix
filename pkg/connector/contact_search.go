// corten-matrix - A Matrix-iMessage puppeting bridge.
// Copyright (C) 2024 Ludvig Rhodin
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package connector

// Contact search and listing for the provisioning API — what Beeper's
// new-chat screen calls when you type a name (search_users) or open the
// contact picker (contacts). iMessage has no server-side user directory, so
// both are backed by the synced address book (iCloud, CardDAV, or local macOS
// Contacts), the same source the `contacts` bot command searches.

import (
	"context"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/lrhodin/corten-matrix/imessage"
)

// SearchUsers finds contacts whose name matches query and returns the
// identifiers that are reachable on iMessage. Like the `contacts` command it
// validates at most maxContactValidate identifiers against IDS per query, so a
// broad search can't hammer Apple — clients call this as the user types.
func (c *IMClient) SearchUsers(ctx context.Context, query string) ([]*bridgev2.ResolveIdentifierResponse, error) {
	if c.client == nil {
		return nil, bridgev2.ErrNotLoggedIn
	}
	if c.contacts == nil {
		return nil, nil
	}

	candidates := rankContactCandidates(c.contacts.GetAllContacts(), query)
	if len(candidates) > maxContactValidate {
		candidates = candidates[:maxContactValidate]
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	ids := make([]string, len(candidates))
	for i, cand := range candidates {
		ids[i] = cand.identifier
	}
	validSet := make(map[string]bool, len(ids))
	for _, v := range c.validateTargetsSafe(ids) {
		validSet[v] = true
	}

	var results []*bridgev2.ResolveIdentifierResponse
	for _, cand := range candidates {
		if !validSet[cand.identifier] {
			continue
		}
		userID := makeUserID(cand.identifier)
		ghost, err := c.Main.Bridge.GetGhostByID(ctx, userID)
		if err != nil {
			return nil, err
		}
		userInfo, err := c.GetUserInfo(ctx, ghost)
		if err != nil {
			return nil, err
		}
		resp := &bridgev2.ResolveIdentifierResponse{
			Ghost:    ghost,
			UserID:   userID,
			UserInfo: userInfo,
		}
		// Surface an existing DM so the client can jump straight to it, but
		// don't create portal rows for every search hit — the chat is created
		// through ResolveIdentifier once the user actually picks someone.
		portalKey := networkid.PortalKey{ID: networkid.PortalID(cand.identifier), Receiver: c.UserLogin.ID}
		if portal, err := c.Main.Bridge.GetExistingPortalByKey(ctx, portalKey); err != nil {
			return nil, err
		} else if portal != nil && portal.MXID != "" {
			resp.Chat = &bridgev2.CreateChatResponse{Portal: portal, PortalKey: portalKey}
		}
		results = append(results, resp)
	}
	return results, nil
}

// GetContactList returns one entry per synced contact, keyed by its canonical
// handle. Entries are not validated against IDS — an address book can hold
// thousands of contacts — so some may not be on iMessage; ResolveIdentifier
// validates the handle when the user starts a chat. No ghosts are loaded for
// the same reason.
func (c *IMClient) GetContactList(ctx context.Context) ([]*bridgev2.ResolveIdentifierResponse, error) {
	if c.client == nil {
		return nil, bridgev2.ErrNotLoggedIn
	}
	if c.contacts == nil {
		return nil, nil
	}

	all := c.contacts.GetAllContacts()
	results := make([]*bridgev2.ResolveIdentifierResponse, 0, len(all))
	for _, contact := range all {
		if resp := c.contactListEntry(contact); resp != nil {
			results = append(results, resp)
		}
	}
	return results, nil
}

// contactListEntry builds the contact-list entry for one address-book contact,
// or nil if the contact has no usable phone or email.
func (c *IMClient) contactListEntry(contact *imessage.Contact) *bridgev2.ResolveIdentifierResponse {
	ids := contactPortalIDs(contact)
	if len(ids) == 0 {
		return nil
	}
	// Match GetUserInfo: when the contact has a phone, list only tel: IDs, so
	// clients treat it as a phone contact rather than picking the email.
	var phones []string
	for _, id := range ids {
		if strings.HasPrefix(id, "tel:") {
			phones = append(phones, id)
		}
	}
	if len(phones) > 0 {
		ids = phones
	}
	primary := pickCanonicalHandle(ids)

	var name string
	if contact.HasName() {
		name = c.Main.Config.FormatDisplayname(DisplaynameParams{
			FirstName: contact.FirstName,
			LastName:  contact.LastName,
			Nickname:  contact.Nickname,
			ID:        stripIdentifierPrefix(primary),
		})
	} else {
		name = c.Main.Config.FormatDisplayname(identifierToDisplaynameParams(primary))
	}

	isBot := false
	return &bridgev2.ResolveIdentifierResponse{
		UserID: makeUserID(primary),
		UserInfo: &bridgev2.UserInfo{
			Name:        &name,
			Identifiers: ids,
			IsBot:       &isBot,
		},
	}
}
