#!/usr/bin/env bash
# Checks that the npm and crates.io versions are valid semver and equal, and
# that the spec minorVersion is not lower than the committed contract snapshot.
#
# Environment overrides: TS_PACKAGE, TS_LOCK, RUST_MANIFEST, SPEC_FILE, SNAPSHOT_FILE.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

TS_PACKAGE="${TS_PACKAGE:-ts/package.json}"
TS_LOCK="${TS_LOCK:-ts/package-lock.json}"
RUST_MANIFEST="${RUST_MANIFEST:-rust/Cargo.toml}"
SPEC_FILE="${SPEC_FILE:-api/filesystem.yaml}"
SNAPSHOT_FILE="${SNAPSHOT_FILE:-api/contract-snapshot.json}"

semver='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$'
status=0
fail() { echo "version-check: $*" >&2; status=1; }

ts_version="$(jq -r .version "$TS_PACKAGE")"
rust_version="$(awk '/^\[package\]/{p=1;next} /^\[/{p=0} p && /^version[[:space:]]*=/{gsub(/[" ]/,"",$0); sub(/^version=/,"",$0); print; exit}' "$RUST_MANIFEST")"

[[ "$ts_version" =~ $semver ]] || fail "$TS_PACKAGE version '$ts_version' is not valid semver"
[[ "$rust_version" =~ $semver ]] || fail "$RUST_MANIFEST version '$rust_version' is not valid semver"
if [ "$ts_version" != "$rust_version" ]; then
  fail "version mismatch: $TS_PACKAGE has $ts_version, $RUST_MANIFEST has $rust_version. Set both to the same value (make bump-version NEW=x.y.z keeps them in lockstep)"
fi

if [ -f "$TS_LOCK" ]; then
  lock_version="$(jq -r .version "$TS_LOCK")"
  lock_root_version="$(jq -r '.packages[""].version' "$TS_LOCK")"
  if [ "$lock_version" != "$ts_version" ] || [ "$lock_root_version" != "$ts_version" ]; then
    fail "$TS_LOCK versions ($lock_version, $lock_root_version) differ from $TS_PACKAGE $ts_version. Run: make bump-version or npm install --package-lock-only in ts/"
  fi
fi

spec_minor="$(awk '/^minorVersion:/{print $2; exit}' "$SPEC_FILE")"
snap_minor="$(jq -r .minorVersion "$SNAPSHOT_FILE")"
if ! [[ "$spec_minor" =~ ^[0-9]+$ && "$snap_minor" =~ ^[0-9]+$ ]]; then
  fail "cannot read minorVersion (spec '$spec_minor', snapshot '$snap_minor')"
elif [ "$spec_minor" -lt "$snap_minor" ]; then
  fail "$SPEC_FILE minorVersion $spec_minor is lower than the snapshot minorVersion $snap_minor. Restore the minorVersion"
fi

[ "$status" -eq 0 ] || exit 1
echo "version-check ok ($ts_version, minorVersion $spec_minor)"
