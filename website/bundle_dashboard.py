"""MkDocs hook: copy the standalone dashboard SPA into the built site at
/dashboard/app/. The MkDocs page docs/dashboard.md (-> /dashboard/) embeds it in
chrome-less mode (?embed=1) so the site's header/footer/nav wrap it; the SPA
itself (dashboard/ at the repo root) is dropped in verbatim under app/."""

import os
import shutil

# repo_root/website/bundle_dashboard.py -> repo_root/dashboard
_DASHBOARD_SRC = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "dashboard")
)


def on_post_build(config, **kwargs):
    dst = os.path.join(config["site_dir"], "dashboard", "app")
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
