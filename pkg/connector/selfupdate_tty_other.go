// corten-matrix - A Matrix-iMessage puppeting bridge.

//go:build !unix

package connector

import "os"

// stdinIsTerminal reports whether a person is at the keyboard.
func stdinIsTerminal() bool {
	fi, err := os.Stdin.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
