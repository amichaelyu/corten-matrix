package connector

import (
	"reflect"
	"testing"
	"time"

	"github.com/lrhodin/corten-matrix/imessage"
)

// fakeChatListAPI stubs only GetChatsWithMessagesAfter; any other
// imessage.API call panics on the nil embedded interface.
type fakeChatListAPI struct {
	imessage.API
	chats []imessage.ChatIdentifier
}

func (f *fakeChatListAPI) GetChatsWithMessagesAfter(time.Time) ([]imessage.ChatIdentifier, error) {
	return f.chats, nil
}

func TestFindGroupChatGUIDsByGroupID(t *testing.T) {
	db := &chatDB{api: &fakeChatListAPI{chats: []imessage.ChatIdentifier{
		{ChatGUID: "any;+;chat444808906265031779", ThreadID: "12421709-51C6-4DB3-AD90-22BBD555C870"},
		{ChatGUID: "any;-;+15178036710"},
		{ChatGUID: "SMS;+;chat444808906265031779", ThreadID: "12421709-51C6-4DB3-AD90-22BBD555C870"},
		{ChatGUID: "any;+;chat9781599630404003", ThreadID: "7B4546BA-865E-4860-B99E-BCA8EA6D1173"},
	}}}

	got := db.findGroupChatGUIDsByGroupID("12421709-51c6-4db3-ad90-22bbd555c870")
	want := []string{"any;+;chat444808906265031779", "SMS;+;chat444808906265031779"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("findGroupChatGUIDsByGroupID() = %#v, want %#v", got, want)
	}
	if got := db.findGroupChatGUIDsByGroupID(""); got != nil {
		t.Fatalf("findGroupChatGUIDsByGroupID(\"\") = %#v, want nil", got)
	}
	if got := db.findGroupChatGUIDsByGroupID("00000000-0000-0000-0000-000000000000"); got != nil {
		t.Fatalf("unknown group = %#v, want nil", got)
	}
}

func TestDropMessagesBefore(t *testing.T) {
	cutoff := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	msgs := []*imessage.Message{
		{GUID: "old", Time: cutoff.Add(-time.Hour)},
		{GUID: "edge", Time: cutoff},
		{GUID: "new", Time: cutoff.Add(time.Hour)},
	}

	got, dropped := dropMessagesBefore(msgs, cutoff)
	if dropped != 1 || len(got) != 2 || got[0].GUID != "edge" || got[1].GUID != "new" {
		t.Fatalf("dropMessagesBefore() kept %d, dropped %d; want edge+new kept, 1 dropped", len(got), dropped)
	}
	if got, dropped := dropMessagesBefore(msgs, cutoff.Add(-24*time.Hour)); dropped != 0 || len(got) != 3 {
		t.Fatalf("all within window: %d kept, %d dropped; want 3, 0", len(got), dropped)
	}
	if got, dropped := dropMessagesBefore(msgs, cutoff.Add(24*time.Hour)); dropped != 3 || len(got) != 0 {
		t.Fatalf("all outside window: %d kept, %d dropped; want 0, 3", len(got), dropped)
	}
	if got, dropped := dropMessagesBefore(nil, cutoff); dropped != 0 || len(got) != 0 {
		t.Fatalf("nil input: %d kept, %d dropped; want 0, 0", len(got), dropped)
	}
}
