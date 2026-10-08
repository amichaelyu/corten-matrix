package connector

import (
	"reflect"
	"testing"

	"github.com/lrhodin/corten-matrix/imessage"
)

func TestContactPortalIDsNormalizesDedupesAndSkipsBlankEmails(t *testing.T) {
	contact := &imessage.Contact{
		Phones: []string{
			"(555) 123-4567",
			"+1 555 123 4567",
			"555.765.4321",
		},
		Emails: []string{
			" USER@example.COM ",
			"",
			"user@example.com",
			"other@example.com",
		},
	}

	got := contactPortalIDs(contact)
	want := []string{
		"tel:+15551234567",
		"tel:+15557654321",
		"mailto:user@example.com",
		"mailto:other@example.com",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("contactPortalIDs() = %#v, want %#v", got, want)
	}
}

func TestContactPortalIDsHandlesNilContact(t *testing.T) {
	if got := contactPortalIDs(nil); got != nil {
		t.Fatalf("contactPortalIDs(nil) = %#v, want nil", got)
	}
}

func TestPickSendTargetPrimaryValid(t *testing.T) {
	portalID := "tel:+15551234567"
	altIDs := []string{"tel:+15557654321", "mailto:user@example.com"}
	validSet := map[string]struct{}{
		portalID:                  {},
		"tel:+15557654321":        {},
		"mailto:user@example.com": {},
	}

	got, ok := pickSendTarget(portalID, altIDs, validSet)
	if !ok || got != portalID {
		t.Fatalf("pickSendTarget() = (%q, %v), want (%q, true)", got, ok, portalID)
	}
}

func TestPickSendTargetFirstValidAlternate(t *testing.T) {
	portalID := "tel:+15551234567"
	altIDs := []string{"tel:+15557654321", "mailto:user@example.com"}
	validSet := map[string]struct{}{
		"mailto:user@example.com": {},
		"tel:+15557654321":        {},
	}

	got, ok := pickSendTarget(portalID, altIDs, validSet)
	if !ok || got != "tel:+15557654321" {
		t.Fatalf("pickSendTarget() = (%q, %v), want (%q, true)", got, ok, "tel:+15557654321")
	}
}

func TestPickSendTargetNothingValid(t *testing.T) {
	portalID := "tel:+15551234567"
	altIDs := []string{"tel:+15557654321", "mailto:user@example.com"}

	got, ok := pickSendTarget(portalID, altIDs, nil)
	if ok || got != portalID {
		t.Fatalf("pickSendTarget() = (%q, %v), want (%q, false)", got, ok, portalID)
	}
}

func TestValidateTargetsSafeGuardsNilClientAndEmptyTargets(t *testing.T) {
	c := &IMClient{}
	if got := c.validateTargetsSafe([]string{"tel:+15551234567"}); got != nil {
		t.Fatalf("validateTargetsSafe() with nil client = %#v, want nil", got)
	}
	if got := c.validateTargetsSafe(nil); got != nil {
		t.Fatalf("validateTargetsSafe(nil) = %#v, want nil", got)
	}
}

func TestContactKeyFromContactMergesPhoneAndEmail(t *testing.T) {
	contact := &imessage.Contact{
		FirstName: "Mom",
		Phones:    []string{"(517) 803-6710"},
		Emails:    []string{"Zhengya3@gmail.com"},
	}
	got := contactKeyFromContact(contact)
	want := "mailto:zhengya3@gmail.com|tel:+15178036710"
	if got != want {
		t.Fatalf("contactKeyFromContact() = %q, want %q", got, want)
	}
}

func TestContactKeyFromContactSingleHandleOrNoName(t *testing.T) {
	cases := map[string]*imessage.Contact{
		"nil":           nil,
		"single phone":  {FirstName: "A", Phones: []string{"5551234567"}},
		"single email":  {FirstName: "A", Emails: []string{"a@example.com"}},
		"no name":       {Phones: []string{"5551234567"}, Emails: []string{"a@example.com"}},
		"dup same addr": {FirstName: "A", Phones: []string{"5551234567", "+1 555 123 4567"}},
	}
	for name, contact := range cases {
		if got := contactKeyFromContact(contact); got != "" {
			t.Errorf("%s: contactKeyFromContact() = %q, want \"\"", name, got)
		}
	}
}
