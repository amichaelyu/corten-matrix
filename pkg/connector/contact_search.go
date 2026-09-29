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

// SearchUsers finds contacts whose name matches query. Clients call it as the
// user types, so it makes no IDS lookups: validating every keystroke's matches
// burns Apple's lookup budget (a new device is rate-limited hard) and, while
// throttled, hides every contact. ResolveIdentifier validates the handle when
// the user picks one. Results are capped at maxContactValidate, ranked like the
// `contacts` command, and no ghost or portal rows are created for them.
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

	var results []*bridgev2.ResolveIdentifierResponse
	for _, cand := range candidates {
		name := c.contactDisplayName(cand.contact, cand.identifier)
		isBot := false
		resp := &bridgev2.ResolveIdentifierResponse{
			UserID: makeUserID(cand.identifier),
			UserInfo: &bridgev2.UserInfo{
				Name:        &name,
				Identifiers: []string{cand.identifier},
				IsBot:       &isBot,
			},
		}
		// Surface an existing DM so the client can jump straight to it.
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

	name := c.contactDisplayName(contact, primary)
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

// contactDisplayName formats a contact's name the way GetUserInfo does, falling
// back to the identifier when the contact has no name.
func (c *IMClient) contactDisplayName(contact *imessage.Contact, identifier string) string {
	if contact.HasName() {
		return c.Main.Config.FormatDisplayname(DisplaynameParams{
			FirstName: contact.FirstName,
			LastName:  contact.LastName,
			Nickname:  contact.Nickname,
			ID:        stripIdentifierPrefix(identifier),
		})
	}
	return c.Main.Config.FormatDisplayname(identifierToDisplaynameParams(identifier))
}
