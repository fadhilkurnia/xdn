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

# 3. Bundle the nightly-perf trend page at /performance/app/, embedded by the
# Performance tab (website/docs/performance.md) the same way the dashboard SPA
# is. The page is static; its data is fetched at view time from the
# perf-results branch (raw.githubusercontent), so nightly data updates need no
# site rebuild. /perf/ redirects to the tab for links minted before the move.
mkdir -p "$ROOT/site/performance/app" "$ROOT/site/perf"
cp "$ROOT/eval/perf-dashboard/index.html" "$ROOT/site/performance/app/index.html"
cat > "$ROOT/site/perf/index.html" <<'REDIR'
<!DOCTYPE html><html><head><meta charset="utf-8">
<meta http-equiv="refresh" content="0; url=/performance/">
<link rel="canonical" href="https://xdn.cs.umass.edu/performance/">
<title>Moved</title></head>
<body>Moved to <a href="/performance/">/performance/</a>.</body></html>
REDIR
echo "bundled perf dashboard -> site/performance/app/ (redirect at /perf/)"

echo "built site -> $ROOT/site  (open: python3 -m http.server -d site)"
