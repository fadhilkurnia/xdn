"""MkDocs hook: copy the standalone dashboard SPA into the built site at
/dashboard/. The dashboard is a separate static app (dashboard/ at the repo
root), not a MkDocs-rendered page, so we drop it in verbatim after the build."""

import os
import shutil

# repo_root/website/bundle_dashboard.py -> repo_root/dashboard
_DASHBOARD_SRC = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "dashboard")
)


def on_post_build(config, **kwargs):
    dst = os.path.join(config["site_dir"], "dashboard")
    if not os.path.isdir(_DASHBOARD_SRC):
        print(f"[bundle_dashboard] WARNING: {_DASHBOARD_SRC} not found; skipping")
        return
    shutil.rmtree(dst, ignore_errors=True)
    # Only the runtime assets, not PLAN.md / dev notes.
    shutil.copytree(
        _DASHBOARD_SRC,
        dst,
        ignore=shutil.ignore_patterns("PLAN.md", "*.md", ".*"),
    )
    print(f"[bundle_dashboard] copied dashboard -> {dst}")
