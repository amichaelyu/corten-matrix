package connector

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2"

	"github.com/lrhodin/corten-matrix/imessage"
)

func TestRankContactCandidatesOrdersExactBeforeFuzzyAndDedupes(t *testing.T) {
	contacts := []*imessage.Contact{
		{FirstName: "Jon", LastName: "Smyth", Phones: []string{"555-000-0001"}},
		{FirstName: "John", LastName: "Smith", Phones: []string{"(555) 000-0002", "+1 555 000 0002"}, Emails: []string{" John@Example.com "}},
		{FirstName: "Alice", LastName: "Jones", Phones: []string{"555-000-0003"}},
	}

	got := rankContactCandidates(contacts, "  JOHN ")
	var ids []string
	for _, c := range got {
		ids = append(ids, c.identifier)
	}
	want := []string{"tel:+15550000002", "mailto:john@example.com", "tel:+15550000001"}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("rankContactCandidates() identifiers = %#v, want %#v", ids, want)
	}
	if got[0].name != "John Smith" {
		t.Fatalf("first candidate name = %q, want %q", got[0].name, "John Smith")
	}
}

func TestRankContactCandidatesEmptyQuery(t *testing.T) {
	contacts := []*imessage.Contact{{FirstName: "John", Phones: []string{"555-000-0001"}}}
	if got := rankContactCandidates(contacts, "   "); got != nil {
		t.Fatalf("rankContactCandidates(blank) = %#v, want nil", got)
	}
}

func TestPickCanonicalHandlePrefersLowestPhone(t *testing.T) {
	tests := []struct {
		ids  []string
		want string
	}{
		{[]string{"mailto:b@example.com", "tel:+15550000002", "tel:+15550000001"}, "tel:+15550000001"},
		{[]string{"mailto:b@example.com", "mailto:a@example.com"}, "mailto:a@example.com"},
		{nil, ""},
	}
	for _, tt := range tests {
		if got := pickCanonicalHandle(tt.ids); got != tt.want {
			t.Errorf("pickCanonicalHandle(%v) = %q, want %q", tt.ids, got, tt.want)
		}
	}
}

func contactListTestClient() *IMClient {
	cfg := IMConfig{DisplaynameTemplate: `{{if .FirstName}}{{.FirstName}}{{if .LastName}} {{.LastName}}{{end}}{{else if .Nickname}}{{.Nickname}}{{else if .Phone}}{{.Phone}}{{else if .Email}}{{.Email}}{{else}}{{.ID}}{{end}}`}
	cfg.PostProcess()
	return &IMClient{Main: &IMConnector{Config: cfg}}
}

func TestContactListEntryListsOnlyPhonesWhenContactHasOne(t *testing.T) {
	c := contactListTestClient()
	resp := c.contactListEntry(&imessage.Contact{
		FirstName: "John",
		LastName:  "Smith",
		Phones:    []string{"555-000-0002", "555-000-0001"},
		Emails:    []string{"john@example.com"},
	})
	if resp == nil {
		t.Fatal("contactListEntry() = nil, want an entry")
	}
	if resp.UserID != makeUserID("tel:+15550000001") {
		t.Fatalf("UserID = %q, want the lowest phone", resp.UserID)
	}
	if want := []string{"tel:+15550000001", "tel:+15550000002"}; !reflect.DeepEqual(resp.UserInfo.Identifiers, want) {
		t.Fatalf("Identifiers = %#v, want %#v", resp.UserInfo.Identifiers, want)
	}
	if *resp.UserInfo.Name != "John Smith" {
		t.Fatalf("Name = %q, want %q", *resp.UserInfo.Name, "John Smith")
	}
}

func TestContactListEntryEmailOnlyAndEmptyContacts(t *testing.T) {
	c := contactListTestClient()
	resp := c.contactListEntry(&imessage.Contact{Emails: []string{"Someone@Example.com"}})
	if resp == nil || resp.UserID != makeUserID("mailto:someone@example.com") {
		t.Fatalf("contactListEntry(email-only) = %+v, want mailto:someone@example.com", resp)
	}
	if *resp.UserInfo.Name == "" {
		t.Fatal("email-only contact got an empty name")
	}
	if got := c.contactListEntry(&imessage.Contact{FirstName: "Nobody"}); got != nil {
		t.Fatalf("contactListEntry(no handles) = %+v, want nil", got)
	}
}

func TestContactSearchRequiresLogin(t *testing.T) {
	c := &IMClient{}
	if _, err := c.SearchUsers(context.Background(), "john"); !errors.Is(err, bridgev2.ErrNotLoggedIn) {
		t.Fatalf("SearchUsers() err = %v, want ErrNotLoggedIn", err)
	}
	if _, err := c.GetContactList(context.Background()); !errors.Is(err, bridgev2.ErrNotLoggedIn) {
		t.Fatalf("GetContactList() err = %v, want ErrNotLoggedIn", err)
	}
}

// ResolveIdentifier normalizes the bare handles provisioning clients send, and
// the start-chat command passes already-normalized ones — both must land on
// the same prefixed identifier.
func TestNormalizeStartChatIdentifierIsIdempotent(t *testing.T) {
	for _, raw := range []string{"+17203529408", "tel:+17203529408", "Someone@Example.com", "mailto:someone@example.com"} {
		once := normalizeStartChatIdentifier(raw)
		if twice := normalizeStartChatIdentifier(once); twice != once {
			t.Errorf("normalizeStartChatIdentifier(%q) = %q, but normalizing again gave %q", raw, once, twice)
		}
		if !strings.HasPrefix(once, "tel:") && !strings.HasPrefix(once, "mailto:") {
			t.Errorf("normalizeStartChatIdentifier(%q) = %q, want a tel: or mailto: identifier", raw, once)
		}
	}
	if got := normalizeStartChatIdentifier("+17203529408"); got != "tel:+17203529408" {
		t.Errorf("bare phone normalized to %q, want tel:+17203529408", got)
	}
}

func TestRankContactCandidatesKeepContact(t *testing.T) {
	john := &imessage.Contact{FirstName: "John", Phones: []string{"555-000-0001"}, Emails: []string{"j@example.com"}}
	for _, cand := range rankContactCandidates([]*imessage.Contact{john}, "john") {
		if cand.contact != john {
			t.Fatalf("candidate %q lost its contact", cand.identifier)
		}
	}
}
