// corten-matrix - A Matrix-iMessage puppeting bridge.

package connector

import "os"

// HandleHostCommand lets the build configuration's host-command extensions
// claim a management subcommand before the binary's normal dispatch. args is
// os.Args[1:]; version/goos/goarch describe the running build. An extension
// that handles a command acts on it — usually terminating the process itself —
// and returns true; otherwise it returns false and normal dispatch proceeds.
//
// This build registers one command: `update`, the self-updater in
// selfupdate.go, which follows this fork's GitHub releases.
func HandleHostCommand(args []string, version, goos, goarch string) bool {
	if len(args) == 0 || args[0] != "update" {
		return false
	}
	os.Exit(runUpdate(args[1:], version, goos, goarch, os.Stdout))
	return true
}

// ExtraHostHelp returns extra {command, description} rows for the `help`
// listing.
func ExtraHostHelp() [][2]string {
	return [][2]string{
		{"update", "update to the latest release & restart the bridge"},
		{"update check", "show the latest release + notes, change nothing"},
		{"update force", "re-download & reinstall the latest release"},
	}
}
