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
