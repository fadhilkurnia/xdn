#!/usr/bin/env bash
# Build the unified xdn.cs.umass.edu site: homepage + docs + dashboard, all from
# this repo, into ./site/.
#
#   homepage / getting-started  <- website/docs/*.md (MkDocs Material)
#   Documentation section       <- repo docs/*.md, staged into website/docs/reference/
#   /dashboard/                 <- dashboard/ SPA (copied by website/bundle_dashboard.py)
#
# Requires: mkdocs-material (`pip install mkdocs-material`).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# 1. Stage the repo's top-level dev docs as the site's Documentation section.
REF="$ROOT/website/docs/reference"
rm -rf "$REF"; mkdir -p "$REF"
find "$ROOT/docs" -maxdepth 1 -name '*.md' -exec cp {} "$REF/" \;
echo "staged $(ls "$REF" | wc -l | tr -d ' ') reference docs"

# 2. Build (bundle_dashboard.py hook drops dashboard/ into the output at /dashboard/).
MKDOCS="${MKDOCS:-mkdocs}"
( cd "$ROOT/website" && "$MKDOCS" build --site-dir "$ROOT/site" )

echo "built site -> $ROOT/site  (open: python3 -m http.server -d site)"
