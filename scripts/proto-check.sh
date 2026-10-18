#!/usr/bin/env bash
# Regenerates the protoc outputs into a temp directory from the proto and fails
# on any difference from the committed files. The protoc version header line is
# ignored because the generated body does not depend on it.
#
# Environment overrides (used by tests and non-standard layouts):
#   PROTO_FILE    proto source, default api/proto/mountos_fs.proto
#   GO_WIRE_DIR   committed Go output dir, default go/wire
#   TS_GEN_DIR    committed TS output dir, default ts/src/gen
#   PROTOC, PROTOC_GEN_GO
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

PROTOC="${PROTOC:-protoc}"
PROTOC_GEN_GO="${PROTOC_GEN_GO:-$HOME/go/bin/protoc-gen-go}"
PROTO_FILE="${PROTO_FILE:-api/proto/mountos_fs.proto}"
GO_WIRE_DIR="${GO_WIRE_DIR:-go/wire}"
TS_GEN_DIR="${TS_GEN_DIR:-ts/src/gen}"
TS_PLUGIN="ts/node_modules/.bin/protoc-gen-ts_proto"
GO_MODULE="github.com/mountos-io/mountos-native-sdk/go"

die() { echo "proto-check: $*" >&2; exit 1; }

command -v "$PROTOC" >/dev/null 2>&1 || die "protoc not found; install protoc"
[ -x "$PROTOC_GEN_GO" ] || die "protoc-gen-go not found at $PROTOC_GEN_GO; run: go install google.golang.org/protobuf/cmd/protoc-gen-go"
[ -x "$TS_PLUGIN" ] || die "protoc-gen-ts_proto not found; run: make ts-install"
[ -f "$PROTO_FILE" ] || die "$PROTO_FILE not found"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/go" "$tmp/ts"

proto_dir="$(dirname "$PROTO_FILE")"
"$PROTOC" --plugin="protoc-gen-go=$PROTOC_GEN_GO" --proto_path="$proto_dir" \
  --go_out="$tmp/go" --go_opt="module=$GO_MODULE" "$PROTO_FILE" \
  || die "protoc Go generation failed"

# Keep these options identical to the gen script in ts/package.json.
"$PROTOC" --plugin="protoc-gen-ts_proto=$TS_PLUGIN" --proto_path="$proto_dir" \
  --ts_proto_out="$tmp/ts" \
  --ts_proto_opt=outputServices=none,esModuleInterop=true,useOptionals=messages,forceLong=bigint,importSuffix=.js,env=node,outputJsonMethods=false,outputPartialMethods=false,outputClientImpl=false \
  "$PROTO_FILE" || die "protoc TS generation failed"

status=0
compare() {
  local label="$1" want="$2" got="$3"
  if [ ! -f "$got" ]; then
    echo "proto-check: $label: committed file $got is missing" >&2
    status=1
    return
  fi
  if ! diff -u <(grep -v -E '^// +protoc +v' "$want") <(grep -v -E '^// +protoc +v' "$got") >"$tmp/diff.txt"; then
    echo "proto-check: $label differs from the output generated from $PROTO_FILE" >&2
    head -40 "$tmp/diff.txt" >&2
    status=1
  fi
}

for f in "$tmp"/go/wire/*.pb.go; do
  compare "Go wire" "$f" "$GO_WIRE_DIR/$(basename "$f")"
done
for f in "$tmp"/ts/*.ts; do
  compare "TS gen" "$f" "$TS_GEN_DIR/$(basename "$f")"
done

if [ "$status" -ne 0 ]; then
  echo "proto-check: fix with: make api-gen (regenerates the proto, Go wire and TS gen)" >&2
  exit 1
fi
echo "proto-check ok"
