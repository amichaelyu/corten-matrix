// corten-matrix - A Matrix-iMessage puppeting bridge.

//go:build unix

package connector

import (
	"os"

	"golang.org/x/sys/unix"
)

// stdinIsTerminal reports whether a person is at the keyboard. A window-size
// ioctl succeeds only on a real terminal; /dev/null is a character device
// too, so a mode check alone would call an unattended run interactive.
func stdinIsTerminal() bool {
	_, err := unix.IoctlGetWinsize(int(os.Stdin.Fd()), unix.TIOCGWINSZ)
	return err == nil
}
