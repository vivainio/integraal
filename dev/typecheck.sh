#!/usr/bin/env bash
# Type-check Luau scripts against the native API definitions (needs luau-lsp).
# Usage: dev/typecheck.sh [files...]   (default: every .luau under examples/)
set -euo pipefail
cd "$(dirname "$0")/.."
files=("$@")
[ ${#files[@]} -eq 0 ] && files=($(find examples -name '*.luau'))
exec luau-lsp analyze --no-strict-dm-types --defs="$PWD/types/ig.d.luau" --defs="$PWD/types/testing.d.luau" --base-luaurc .luaurc "${files[@]}"
