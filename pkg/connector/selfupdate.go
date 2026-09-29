// corten-matrix - A Matrix-iMessage puppeting bridge.

package connector

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Self-updater behind `corten-matrix update`.
//
// The flow mirrors what the README documents for the official binaries: fetch
// the latest GitHub release for this fork, pick the asset for the running
// platform, download it next to the installed binary, verify it against the
// release's checksum file when one is published, swap it in with two renames
// (the running process keeps its old inode, so nothing crashes mid-update),
// sanity-check the new binary, print the release notes, and restart the
// service. `update check` only reports; `update force` reinstalls even when
// the versions already match.

// defaultUpdateRepo is the GitHub repository whose releases the updater
// follows. CORTEN_UPDATE_REPO overrides it at runtime so a fork of the fork
// can point the updater at itself without a rebuild.
const defaultUpdateRepo = "amichaelyu/corten-matrix"

// updateAPIBase is the GitHub REST endpoint root. A var so tests can point it
// at an httptest server.
var updateAPIBase = "https://api.github.com"

// updateHTTPClient is the client used for the release lookup and the asset
// download. The download of a ~120MB binary can take a while on a slow link,
// so the transport has no overall timeout; per-phase contexts bound it instead.
var updateHTTPClient = &http.Client{}

// githubRelease is the subset of GitHub's release object the updater reads.
type githubRelease struct {
	TagName     string         `json:"tag_name"`
	Name        string         `json:"name"`
	Body        string         `json:"body"`
	HTMLURL     string         `json:"html_url"`
	Prerelease  bool           `json:"prerelease"`
	Draft       bool           `json:"draft"`
	PublishedAt string         `json:"published_at"`
	Assets      []releaseAsset `json:"assets"`
}

type releaseAsset struct {
	Name        string `json:"name"`
	DownloadURL string `json:"browser_download_url"`
	Size        int64  `json:"size"`
}

func updateRepo() string {
	if r := strings.TrimSpace(os.Getenv("CORTEN_UPDATE_REPO")); r != "" {
		return r
	}
	return defaultUpdateRepo
}

// binaryAssetCandidates lists, in preference order, the release asset names
// that can run on goos/goarch. The first two shapes are what the fork's
// release workflow publishes; the bare `corten-matrix-macos` name is the
// upstream universal-binary asset, kept so the updater also works against a
// release that only ships that.
func binaryAssetCandidates(goos, goarch string) []string {
	switch goos {
	case "darwin":
		return []string{
			"corten-matrix-macos-" + goarch,
			"corten-matrix-macos." + goarch,
			"corten-matrix-macos",
			"corten-matrix-darwin-" + goarch,
		}
	case "linux":
		return []string{
			"corten-matrix-linux-" + goarch,
			"corten-matrix-linux." + goarch,
		}
	}
	return nil
}

// pickReleaseAsset selects the binary asset for this platform and, when the
// release publishes one, the checksum file that covers it.
func pickReleaseAsset(assets []releaseAsset, goos, goarch string) (bin *releaseAsset, sums *releaseAsset) {
	byName := make(map[string]*releaseAsset, len(assets))
	for i := range assets {
		byName[assets[i].Name] = &assets[i]
	}
	for _, name := range binaryAssetCandidates(goos, goarch) {
		if a, ok := byName[name]; ok {
			bin = a
			break
		}
	}
	if bin == nil {
		return nil, nil
	}
	for _, name := range []string{bin.Name + ".sha256", "SHA256SUMS", "SHA256SUMS.txt", "checksums.txt", "sha256sums.txt"} {
		if a, ok := byName[name]; ok {
			return bin, a
		}
	}
	return bin, nil
}

// parseSHA256Sums reads `sha256sum`-style output ("<hex>  <name>" per line,
// a leading `*` on the name for binary mode) and returns the digest recorded
// for name, or "" when the file does not mention it. A file that holds a bare
// digest with no name (the `<asset>.sha256` shape) matches any name.
func parseSHA256Sums(data, name string) string {
	sc := bufio.NewScanner(strings.NewReader(data))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 1 && len(fields[0]) == 64 {
			return strings.ToLower(fields[0])
		}
		if len(fields) < 2 || len(fields[0]) != 64 {
			continue
		}
		file := strings.TrimPrefix(fields[len(fields)-1], "*")
		if filepath.Base(file) == name {
			return strings.ToLower(fields[0])
		}
	}
	return ""
}

// parseSemver splits "v?MAJOR.MINOR.PATCH[-pre][+build]" into its numeric
// parts. ok is false for anything else (a "unknown" or commit-hash Tag, for
// example), which the updater treats as older than every release.
func parseSemver(v string) (parts [3]int, pre string, ok bool) {
	v = strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(v, "v"), "V"))
	if i := strings.IndexByte(v, '+'); i >= 0 {
		v = v[:i]
	}
	if i := strings.IndexByte(v, '-'); i >= 0 {
		pre = v[i+1:]
		v = v[:i]
	}
	nums := strings.Split(v, ".")
	if len(nums) != 3 {
		return parts, "", false
	}
	for i, n := range nums {
		x, err := strconv.Atoi(n)
		if err != nil || x < 0 {
			return parts, "", false
		}
		parts[i] = x
	}
	return parts, pre, true
}

// compareVersions orders two version strings: -1 when a < b, 0 when equal,
// 1 when a > b. Unparseable versions sort before every parseable one, and
// a pre-release sorts before its final release.
func compareVersions(a, b string) int {
	pa, prea, oka := parseSemver(a)
	pb, preb, okb := parseSemver(b)
	switch {
	case !oka && !okb:
		return strings.Compare(a, b)
	case !oka:
		return -1
	case !okb:
		return 1
	}
	for i := 0; i < 3; i++ {
		if pa[i] != pb[i] {
			if pa[i] < pb[i] {
				return -1
			}
			return 1
		}
	}
	switch {
	case prea == preb:
		return 0
	case prea == "":
		return 1
	case preb == "":
		return -1
	}
	return strings.Compare(prea, preb)
}

// installedBinaryPath finds the binary the update replaces: the
// `corten-matrix` on PATH (the /usr/local/bin symlink setup creates),
// resolved through its symlinks to the real file. When nothing is on PATH it
// falls back to the running executable, so a binary that was never symlinked
// can still update itself in place.
func installedBinaryPath() (path string, viaPath bool, err error) {
	if p, lerr := exec.LookPath("corten-matrix"); lerr == nil && p != "" {
		if abs, aerr := filepath.Abs(p); aerr == nil {
			p = abs
		}
		if rp, rerr := filepath.EvalSymlinks(p); rerr == nil {
			return rp, true, nil
		}
		return p, true, nil
	}
	p, err := os.Executable()
	if err != nil {
		return "", false, err
	}
	if rp, rerr := filepath.EvalSymlinks(p); rerr == nil {
		p = rp
	}
	return p, false, nil
}

func githubRequest(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "corten-matrix-updater")
	for _, env := range []string{"GITHUB_TOKEN", "GH_TOKEN"} {
		if tok := os.Getenv(env); tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
			break
		}
	}
	resp, err := updateHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		msg := strings.TrimSpace(string(body))
		if resp.StatusCode == http.StatusForbidden && strings.Contains(strings.ToLower(msg), "rate limit") {
			return nil, fmt.Errorf("GitHub API rate limit hit; set GITHUB_TOKEN and retry")
		}
		return nil, fmt.Errorf("GET %s: HTTP %d: %s", url, resp.StatusCode, msg)
	}
	return resp, nil
}

// fetchLatestRelease returns the newest published, non-draft release of repo.
// GitHub's /releases/latest already excludes drafts and pre-releases.
func fetchLatestRelease(ctx context.Context, repo string) (*githubRelease, error) {
	resp, err := githubRequest(ctx, updateAPIBase+"/repos/"+repo+"/releases/latest")
	if err != nil {
		if strings.Contains(err.Error(), "HTTP 404") {
			return nil, fmt.Errorf("%s has no published release yet (run the \"Build fork release\" workflow on GitHub to make one)", repo)
		}
		return nil, err
	}
	defer resp.Body.Close()
	var rel githubRelease
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&rel); err != nil {
		return nil, fmt.Errorf("decode release: %w", err)
	}
	if rel.TagName == "" {
		return nil, errors.New("release has no tag")
	}
	return &rel, nil
}

// downloadAsset streams url to dst, printing coarse progress, and returns the
// SHA-256 of what was written.
func downloadAsset(ctx context.Context, url, dst string, size int64, out io.Writer) (string, error) {
	resp, err := githubRequest(ctx, url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	var written int64
	lastPct := -1
	buf := make([]byte, 1<<20)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := f.Write(buf[:n]); werr != nil {
				f.Close()
				return "", werr
			}
			h.Write(buf[:n])
			written += int64(n)
			if size > 0 {
				if pct := int(written * 100 / size); pct/10 != lastPct/10 {
					lastPct = pct
					fmt.Fprintf(out, "\r  downloading… %3d%%", pct)
				}
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			f.Close()
			return "", rerr
		}
	}
	if size > 0 {
		fmt.Fprintf(out, "\r  downloaded %s (%.1f MB)\n", filepath.Base(url), float64(written)/1e6)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	if size > 0 && written != size {
		return "", fmt.Errorf("short download: got %d bytes, release says %d", written, size)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// needsSudo reports whether the current user cannot write the directory
// holding path (rename needs directory write permission, not file permission).
func needsSudo(path string) bool {
	dir := filepath.Dir(path)
	probe, err := os.CreateTemp(dir, ".corten-matrix-write-probe-")
	if err != nil {
		return true
	}
	probe.Close()
	_ = os.Remove(probe.Name())
	return false
}

func runQuiet(name string, args ...string) error {
	c := exec.Command(name, args...)
	c.Stdin, c.Stdout, c.Stderr = os.Stdin, os.Stdout, os.Stderr
	return c.Run()
}

// swapBinary moves the freshly downloaded file into place: the old binary is
// kept as <target>.previous for rollback, then the new one is renamed over the
// target. Both steps are renames within one directory, so the running bridge
// keeps executing its already-mapped old inode and the switch is atomic for
// anything that execs the path afterwards.
func swapBinary(target, fresh string, sudo bool) (backup string, err error) {
	backup = target + ".previous"
	if sudo {
		if err := runQuiet("sudo", "mv", "-f", target, backup); err != nil {
			return "", fmt.Errorf("back up old binary: %w", err)
		}
		if err := runQuiet("sudo", "mv", "-f", fresh, target); err != nil {
			_ = runQuiet("sudo", "mv", "-f", backup, target)
			return "", fmt.Errorf("install new binary: %w", err)
		}
		return backup, nil
	}
	if err := os.Rename(target, backup); err != nil {
		return "", fmt.Errorf("back up old binary: %w", err)
	}
	if err := os.Rename(fresh, target); err != nil {
		_ = os.Rename(backup, target)
		return "", fmt.Errorf("install new binary: %w", err)
	}
	return backup, nil
}

func rollbackBinary(target, backup string, sudo bool) error {
	if sudo {
		return runQuiet("sudo", "mv", "-f", backup, target)
	}
	return os.Rename(backup, target)
}

// binaryRuns execs the freshly installed binary with --version so a truncated
// or wrong-architecture download is caught before the service restarts into
// it.
func binaryRuns(path string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, path, "--version").CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// fullDiskAccessOK asks the binary at path whether it can read chat.db. On
// macOS, TCC ties Full Disk Access to the binary's code identity, so a
// replaced binary can lose a grant the old one had; the updater compares
// before and after and warns only when the update itself took it away.
func fullDiskAccessOK(path string) bool {
	if runtime.GOOS != "darwin" {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, path, "fda-check").Run() == nil
}

// serviceInstalled reports whether the bridge runs as a launchd agent or a
// systemd unit on this host, i.e. whether `restart` has anything to restart.
func serviceInstalled() bool {
	home, _ := os.UserHomeDir()
	if runtime.GOOS == "darwin" {
		_, err := os.Stat(filepath.Join(home, "Library", "LaunchAgents", "com.lrhodin.corten-matrix.plist"))
		return err == nil
	}
	for _, p := range []string{
		filepath.Join(home, ".config", "systemd", "user", "corten-matrix.service"),
		"/etc/systemd/system/corten-matrix.service",
	} {
		if _, err := os.Stat(p); err == nil {
			return true
		}
	}
	return false
}

func printReleaseNotes(out io.Writer, rel *githubRelease) {
	title := rel.Name
	if title == "" {
		title = rel.TagName
	}
	fmt.Fprintf(out, "\n  %s (%s)\n", title, rel.TagName)
	if rel.PublishedAt != "" {
		if t, err := time.Parse(time.RFC3339, rel.PublishedAt); err == nil {
			fmt.Fprintf(out, "  published %s\n", t.Local().Format("2006-01-02 15:04"))
		}
	}
	if rel.HTMLURL != "" {
		fmt.Fprintf(out, "  %s\n", rel.HTMLURL)
	}
	if body := strings.TrimSpace(rel.Body); body != "" {
		fmt.Fprintln(out)
		for _, line := range strings.Split(strings.ReplaceAll(body, "\r\n", "\n"), "\n") {
			fmt.Fprintf(out, "    %s\n", line)
		}
	}
	fmt.Fprintln(out)
}

// runUpdate implements `corten-matrix update [check|force]` and returns the
// process exit code.
func runUpdate(args []string, version, goos, goarch string, out io.Writer) int {
	mode := ""
	yes := false
	for _, a := range args {
		switch a {
		case "--yes", "-y":
			yes = true
		case "check", "force":
			mode = a
		default:
			fmt.Fprintf(out, "usage: corten-matrix update [check|force] [--yes]\n")
			return 2
		}
	}
	repo := updateRepo()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	rel, err := fetchLatestRelease(ctx, repo)
	cancel()
	if err != nil {
		fmt.Fprintf(out, "corten-matrix update: cannot read the latest release of %s: %v\n", repo, err)
		return 1
	}
	bin, sums := pickReleaseAsset(rel.Assets, goos, goarch)

	cmp := compareVersions(version, rel.TagName)
	fmt.Fprintf(out, "  installed: %s\n  latest:    %s  (%s)\n", version, rel.TagName, repo)
	if mode == "check" {
		switch {
		case bin == nil:
			fmt.Fprintf(out, "  no %s/%s binary is attached to this release\n", goos, goarch)
		case cmp < 0:
			fmt.Fprintf(out, "  update available — run `corten-matrix update` to install %s\n", bin.Name)
		case cmp == 0:
			fmt.Fprintf(out, "  up to date\n")
		default:
			fmt.Fprintf(out, "  installed build is newer than the latest release\n")
		}
		printReleaseNotes(out, rel)
		return 0
	}
	if bin == nil {
		fmt.Fprintf(out, "corten-matrix update: release %s has no binary for %s/%s\n", rel.TagName, goos, goarch)
		return 1
	}
	if mode != "force" {
		if cmp == 0 {
			fmt.Fprintf(out, "  already up to date (use `update force` to reinstall)\n")
			return 0
		}
		if cmp > 0 {
			fmt.Fprintf(out, "  installed build is newer than the latest release; use `update force` to install %s anyway\n", rel.TagName)
			return 0
		}
	}

	// Installing is a hand-run operation: nothing in the bridge schedules
	// it, and it will not run from a script or a service unless told so
	// explicitly with --yes.
	if !yes && !stdinIsTerminal() {
		fmt.Fprintf(out, "corten-matrix update: refusing to replace the binary without a terminal; run it interactively, or pass --yes to update unattended\n")
		return 2
	}

	target, viaPath, err := installedBinaryPath()
	if err != nil {
		fmt.Fprintf(out, "corten-matrix update: cannot locate the installed binary: %v\n", err)
		return 1
	}
	if !viaPath {
		fmt.Fprintf(out, "  note: corten-matrix is not on PATH; updating the running binary at %s\n", target)
	}
	if fi, err := os.Stat(target); err != nil || fi.IsDir() {
		fmt.Fprintf(out, "corten-matrix update: %s is not a file\n", target)
		return 1
	}
	fmt.Fprintf(out, "  binary:    %s\n", target)

	// Fetch the checksum file first so a bad download is rejected before
	// anything touches the installed binary.
	wantSum := ""
	if sums != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		resp, err := githubRequest(ctx, sums.DownloadURL)
		if err == nil {
			data, rerr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			if rerr == nil {
				wantSum = parseSHA256Sums(string(data), bin.Name)
			}
		}
		cancel()
		if wantSum == "" {
			fmt.Fprintf(out, "  warning: %s does not list %s; skipping checksum verification\n", sums.Name, bin.Name)
		}
	}

	sudo := needsSudo(target)
	stageDir := filepath.Dir(target)
	if sudo {
		stageDir = os.TempDir()
	}
	fresh := filepath.Join(stageDir, ".corten-matrix.update-"+strings.TrimPrefix(rel.TagName, "v")+"-"+strconv.Itoa(os.Getpid()))
	defer os.Remove(fresh)

	fmt.Fprintf(out, "  fetching %s (%.1f MB)\n", bin.Name, float64(bin.Size)/1e6)
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Minute)
	gotSum, err := downloadAsset(ctx, bin.DownloadURL, fresh, bin.Size, out)
	cancel()
	if err != nil {
		fmt.Fprintf(out, "corten-matrix update: download failed: %v\n", err)
		return 1
	}
	if wantSum != "" {
		if gotSum != wantSum {
			fmt.Fprintf(out, "corten-matrix update: checksum mismatch for %s (got %s, release lists %s); not installing\n", bin.Name, gotSum, wantSum)
			return 1
		}
		fmt.Fprintf(out, "  checksum verified\n")
	}
	if err := os.Chmod(fresh, 0o755); err != nil {
		fmt.Fprintf(out, "corten-matrix update: chmod: %v\n", err)
		return 1
	}

	hadFDA := fullDiskAccessOK(target)
	if sudo {
		fmt.Fprintf(out, "  %s is not writable by this user; using sudo for the swap\n", filepath.Dir(target))
	}
	backup, err := swapBinary(target, fresh, sudo)
	if err != nil {
		fmt.Fprintf(out, "corten-matrix update: %v\n", err)
		return 1
	}
	if err := binaryRuns(target); err != nil {
		fmt.Fprintf(out, "corten-matrix update: the new binary does not run (%v); restoring the previous one\n", err)
		if rerr := rollbackBinary(target, backup, sudo); rerr != nil {
			fmt.Fprintf(out, "corten-matrix update: rollback failed: %v — the previous binary is at %s\n", rerr, backup)
		}
		return 1
	}
	if sudo {
		_ = runQuiet("sudo", "rm", "-f", backup)
	} else {
		_ = os.Remove(backup)
	}
	fmt.Fprintf(out, "  installed %s\n", rel.TagName)
	if hadFDA && !fullDiskAccessOK(target) {
		fmt.Fprintf(out, "\n  ⚠ macOS dropped Full Disk Access for the replaced binary. Re-grant it under\n"+
			"    System Settings → Privacy & Security → Full Disk Access, then restart the bridge.\n"+
			"    (open \"x-apple.systempreferences:com.apple.preference.security?Privacy_AllFiles\")\n")
	}
	printReleaseNotes(out, rel)

	if !serviceInstalled() {
		fmt.Fprintf(out, "  no background service is installed; restart the bridge yourself to pick up the new version\n")
		return 0
	}
	fmt.Fprintf(out, "  restarting the bridge…\n")
	if err := runQuiet(target, "restart"); err != nil {
		fmt.Fprintf(out, "corten-matrix update: restart failed (%v); run `corten-matrix restart`\n", err)
		return 1
	}
	return 0
}
