package connector

import (
	"errors"
	"testing"
)

// With iCloud features off, every PET refresh caller (startup share prime,
// the 12h periodic refresh, the throttled variant) must be refused before a
// login is attempted — even without a token provider.
func TestSafeRefreshPetTokenRefusedWhenICloudDisabled(t *testing.T) {
	prev := icloudAutoLoginDisabled
	t.Cleanup(func() { icloudAutoLoginDisabled = prev })

	icloudAutoLoginDisabled = true
	if err := safeRefreshPetToken(nil); !errors.Is(err, errICloudAutoLoginDisabled) {
		t.Fatalf("safeRefreshPetToken() err = %v, want errICloudAutoLoginDisabled", err)
	}
}
