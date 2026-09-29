// corten-matrix - A Matrix-iMessage puppeting bridge.

package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCompareVersions(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"1.2.3", "1.2.3", 0},
		{"v1.2.3", "1.2.3", 0},
		{"1.2.3", "1.2.4", -1},
		{"1.10.0", "1.9.9", 1},
		{"2.0.0", "1.99.99", 1},
		{"1.2.3-rc1", "1.2.3", -1},
		{"1.2.3", "1.2.3-rc1", 1},
		{"1.2.3+abc", "1.2.3", 0},
		{"unknown", "1.0.0", -1},
		{"0.1.0", "unknown", 1},
		{"abc", "abd", -1},
	}
	for _, c := range cases {
		if got := compareVersions(c.a, c.b); got != c.want {
			t.Errorf("compareVersions(%q, %q) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestPickReleaseAsset(t *testing.T) {
	assets := []releaseAsset{
		{Name: "corten-matrix-linux-amd64"},
		{Name: "corten-matrix-linux-arm64"},
		{Name: "corten-matrix-macos"},
		{Name: "corten-matrix-macos-arm64"},
		{Name: "SHA256SUMS"},
	}
	bin, sums := pickReleaseAsset(assets, "darwin", "arm64")
	if bin == nil || bin.Name != "corten-matrix-macos-arm64" {
		t.Fatalf("darwin/arm64 picked %+v", bin)
	}
	if sums == nil || sums.Name != "SHA256SUMS" {
		t.Fatalf("checksum asset = %+v", sums)
	}
	// An x86 Mac has no slice of its own here, so the universal asset wins.
	if bin, _ := pickReleaseAsset(assets, "darwin", "amd64"); bin == nil || bin.Name != "corten-matrix-macos" {
		t.Fatalf("darwin/amd64 picked %+v", bin)
	}
	if bin, _ := pickReleaseAsset(assets, "linux", "arm64"); bin == nil || bin.Name != "corten-matrix-linux-arm64" {
		t.Fatalf("linux/arm64 picked %+v", bin)
	}
	if bin, _ := pickReleaseAsset(assets, "windows", "amd64"); bin != nil {
		t.Fatalf("windows should have no asset, got %+v", bin)
	}
	// A per-asset .sha256 file is preferred over the combined list.
	assets = append(assets, releaseAsset{Name: "corten-matrix-macos-arm64.sha256"})
	if _, sums := pickReleaseAsset(assets, "darwin", "arm64"); sums == nil || sums.Name != "corten-matrix-macos-arm64.sha256" {
		t.Fatalf("per-asset checksum not preferred: %+v", sums)
	}
}

func TestParseSHA256Sums(t *testing.T) {
	const h1 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const h2 = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
	sums := "# release checksums\n" + h1 + "  corten-matrix-macos-arm64\n" + h2 + " *dist/corten-matrix-linux-amd64\n"
	if got := parseSHA256Sums(sums, "corten-matrix-macos-arm64"); got != h1 {
		t.Errorf("macos sum = %q", got)
	}
	if got := parseSHA256Sums(sums, "corten-matrix-linux-amd64"); got != strings.ToLower(h2) {
		t.Errorf("linux sum = %q", got)
	}
	if got := parseSHA256Sums(sums, "corten-matrix-linux-arm64"); got != "" {
		t.Errorf("missing name should be empty, got %q", got)
	}
	if got := parseSHA256Sums(h1+"\n", "anything"); got != h1 {
		t.Errorf("bare digest = %q", got)
	}
}

func TestFetchLatestRelease(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/example/fork/releases/latest" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Accept") != "application/vnd.github+json" {
			t.Errorf("missing Accept header")
		}
		_ = json.NewEncoder(w).Encode(githubRelease{
			TagName: "1.4.0",
			Name:    "corten-matrix 1.4.0",
			Assets:  []releaseAsset{{Name: "corten-matrix-macos-arm64", DownloadURL: "http://x/y", Size: 12}},
		})
	}))
	defer srv.Close()
	old := updateAPIBase
	updateAPIBase = srv.URL
	defer func() { updateAPIBase = old }()

	rel, err := fetchLatestRelease(context.Background(), "example/fork")
	if err != nil {
		t.Fatal(err)
	}
	if rel.TagName != "1.4.0" || len(rel.Assets) != 1 {
		t.Fatalf("unexpected release %+v", rel)
	}
	if _, err := fetchLatestRelease(context.Background(), "example/missing"); err == nil {
		t.Fatal("expected an error for a missing repo")
	}
}

func TestRunUpdateCheck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(githubRelease{
			TagName: "2.0.0",
			Body:    "- fixed things\r\n- more",
			Assets:  []releaseAsset{{Name: "corten-matrix-linux-amd64", Size: 1}},
		})
	}))
	defer srv.Close()
	old := updateAPIBase
	updateAPIBase = srv.URL
	defer func() { updateAPIBase = old }()
	t.Setenv("CORTEN_UPDATE_REPO", "example/fork")

	var out bytes.Buffer
	if code := runUpdate([]string{"check"}, "1.0.0", "linux", "amd64", &out); code != 0 {
		t.Fatalf("exit %d: %s", code, out.String())
	}
	s := out.String()
	for _, want := range []string{"update available", "2.0.0", "fixed things", "corten-matrix-linux-amd64"} {
		if !strings.Contains(s, want) {
			t.Errorf("output missing %q:\n%s", want, s)
		}
	}

	out.Reset()
	if code := runUpdate([]string{"check"}, "2.0.0", "darwin", "arm64", &out); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if !strings.Contains(out.String(), "no darwin/arm64 binary") {
		t.Errorf("expected a missing-asset note:\n%s", out.String())
	}

	// Up to date, no force: nothing is downloaded and the exit is clean.
	out.Reset()
	if code := runUpdate(nil, "2.0.0", "linux", "amd64", &out); code != 0 || !strings.Contains(out.String(), "already up to date") {
		t.Fatalf("exit %d: %s", code, out.String())
	}
	if code := runUpdate([]string{"bogus"}, "2.0.0", "linux", "amd64", &out); code != 2 {
		t.Fatalf("bad mode exit %d", code)
	}
	// An install with no terminal on stdin (this test) is refused before
	// anything is downloaded, unless --yes is given.
	out.Reset()
	if code := runUpdate([]string{"force"}, "2.0.0", "linux", "amd64", &out); code != 2 || !strings.Contains(out.String(), "without a terminal") {
		t.Fatalf("unattended install should be refused: exit %d: %s", code, out.String())
	}
	out.Reset()
	if code := runUpdate(nil, "1.0.0", "linux", "amd64", &out); code != 2 || !strings.Contains(out.String(), "without a terminal") {
		t.Fatalf("unattended update should be refused: exit %d: %s", code, out.String())
	}
}

func TestHandleHostCommandDeclinesOthers(t *testing.T) {
	if HandleHostCommand(nil, "1.0.0", "darwin", "arm64") {
		t.Fatal("empty args should be declined")
	}
	if HandleHostCommand([]string{"restart"}, "1.0.0", "darwin", "arm64") {
		t.Fatal("restart should be declined")
	}
	if rows := ExtraHostHelp(); len(rows) == 0 || rows[0][0] != "update" {
		t.Fatalf("help rows = %v", rows)
	}
}
