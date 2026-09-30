# Source-build patches for the pinned rustpush tree

`make build` applies these unified diffs to the upstream tree it clones at the
SHA in `../rustpush-upstream.sha`, after the one-line patches carried inline
in the Makefile. They exist for changes too large to express as a Makefile
substitution. Each directory is named after the tree it applies to:

| Directory | Apply root |
| --- | --- |
| `apple-private-apis/` | `third_party/rustpush-upstream/third_party/apple-private-apis` |
| `rustpush/` | `third_party/rustpush-upstream` |

`shared-token-refresh-lock.patch` shares a login lock with the wrapper's PET refresh
so a concurrent automatic login must observe the previous attempt's backoff.
`apns-reconnect-spacing.patch` spaces APNs connection attempts at least 30 seconds
apart within a process, including when a successful connection immediately drops.
These limits reduce avoidable retry traffic; they are not Apple-approved limits
or a guarantee against account restrictions.

The Makefile applies every patch through `rp_apply`, which verifies the
desired end state rather than trusting that the patch ran: a patch that is
already fully present is skipped, a tree that carries only part of it stops
the build, and a patch that no longer applies to the pinned tree stops the
build too. A patch that upstream adopts is deleted here together with its
Makefile line, in the same commit.

These are conventional diffs, so they can be reviewed and tried by hand:

```sh
git -C third_party/rustpush-upstream/third_party/apple-private-apis \
  apply --unidiff-zero --whitespace=fix --check \
  "$PWD/third_party/patches/apple-private-apis/<name>.patch"
```
