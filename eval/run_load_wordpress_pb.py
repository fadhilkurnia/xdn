import csv
import os
import subprocess
import sys
import time
from pathlib import Path

import matplotlib.pyplot as plt

from run_load_pb_wordpress_common import (
    ADMIN_PASSWORD,
    ADMIN_USER,
    AR_HOSTS,
    CONTROL_PLANE_HOST,
    HTTP_PROXY_PORT,
    IMAGE_SOURCE_HOST,
    REQUIRED_IMAGES,
    SCREEN_LOG,
    SERVICE_NAME,
    TEST_POSTS,
    TIMEOUT_PORT_SEC,
    TIMEOUT_PRIMARY_SEC,
    TIMEOUT_WP_SEC,
    WORDPRESS_YAML,
    XDN_BINARY,
    check_xmlrpc,
    clear_xdn_cluster,
    create_post,
    detect_primary,
    install_wordpress,
    start_cluster,
    validate_posts,
    wait_for_port,
    wait_for_service,
)

LOAD_RATES = [1, 2, 5, 10, 20, 30, 50]   # req/s
LOAD_DURATION_SEC = 30
MEDIAN_LATENCY_THRESHOLD_MS = 5000
RESULTS_DIR = Path(__file__).resolve().parent / "results"
GO_LATENCY_CLIENT = Path(__file__).resolve().parent / "get_latency_at_rate.go"
OUTPUT_CSV = RESULTS_DIR / "wp_pb_load_latency.csv"
OUTPUT_PLOT = RESULTS_DIR / "wp_pb_load_latency.png"

# wp.newPost payload — one fixed post template reused for every load request.
# Content-Type text/xml is passed via -H to override get_latency_at_rate.go's
# default application/json (the custom -H headers are applied after the hardcoded set).
XMLRPC_NEW_POST_PAYLOAD = (
    '<?xml version="1.0"?>'
    '<methodCall><methodName>wp.newPost</methodName><params>'
    '<param><value><int>1</int></value></param>'
    f'<param><value><string>{ADMIN_USER}</string></value></param>'
    f'<param><value><string>{ADMIN_PASSWORD}</string></value></param>'
    '<param><value><struct>'
    '<member><name>post_title</name>'
    '<value><string>Load Test Post</string></value></member>'
    '<member><name>post_content</name>'
    '<value><string>Benchmark post.</string></value></member>'
    '<member><name>post_status</name>'
    '<value><string>publish</string></value></member>'
    '</struct></value></param>'
    '</params></methodCall>'
)


def run_load_point(primary, rate):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    output_file = RESULTS_DIR / f"wp_pb_rate{rate}.txt"
    url = f"http://{primary}:{HTTP_PROXY_PORT}/xmlrpc.php"
    cmd = [
        "go", "run", str(GO_LATENCY_CLIENT),
        "-H", f"XDN: {SERVICE_NAME}",
        "-H", "Content-Type: text/xml",
        "-X", "POST",
        url,
        XMLRPC_NEW_POST_PAYLOAD,
        str(LOAD_DURATION_SEC),
        str(rate),
    ]
    env = os.environ.copy()
    env["GO111MODULE"] = "off"
    env["GOWORK"] = "off"
    with open(output_file, "w") as fh:
        result = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT,
                                env=env, text=True)
    if result.returncode != 0:
        print(f"   WARNING: Go client exited {result.returncode}")
    return _parse_go_output(output_file)


def _parse_go_output(path):
    metrics = {}
    for line in open(path):
        if ':' not in line:
            continue
        k, v = line.split(':', 1)
        try:
            metrics[k.strip()] = float(v.strip())
        except ValueError:
            pass
    return {
        "throughput_rps": metrics.get("actual_throughput_rps", 0.0),
        "avg_ms":         metrics.get("average_latency_ms", 0.0),
        "p50_ms":         metrics.get("median_latency_ms", 0.0),
        "p90_ms":         metrics.get("p90_latency_ms", 0.0),
        "p95_ms":         metrics.get("p95_latency_ms", 0.0),
        "p99_ms":         metrics.get("p99_latency_ms", 0.0),
    }


def save_csv(rows):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    fields = ["rate_rps", "throughput_rps", "avg_ms", "p50_ms",
              "p90_ms", "p95_ms", "p99_ms"]
    with open(OUTPUT_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    print(f"   CSV saved: {OUTPUT_CSV}")


def plot_results(rows):
    rates = [r["rate_rps"] for r in rows]
    avg   = [r["avg_ms"]   for r in rows]
    p50   = [r["p50_ms"]   for r in rows]
    p95   = [r["p95_ms"]   for r in rows]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(rates, avg, "o-b",  linewidth=2, label="avg")
    ax.plot(rates, p50, "s--g", linewidth=1, label="p50 (median)")
    ax.plot(rates, p95, "^:r",  linewidth=1, label="p95")
    ax.set_xlabel("Offered Load (req/s)", fontsize=12)
    ax.set_ylabel("Latency (ms)",         fontsize=12)
    ax.set_title("WordPress Primary-Backup: Load vs. Latency")
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_PLOT, dpi=150)
    print(f"   Plot saved: {OUTPUT_PLOT}")


if __name__ == '__main__':
    print("=" * 60)
    print("WordPress PB Load-Latency Benchmark")
    print("=" * 60)

    # ── Cluster setup ─────────────────────────────────────────────────────────
    print("\n[Phase 1] Force-clearing cluster ...")
    clear_xdn_cluster()

    print("\n[Phase 2] Starting XDN cluster ...")
    start_cluster()

    print("\n[Phase 3] Waiting for Active Replicas ...")
    for host in AR_HOSTS:
        if not wait_for_port(host, HTTP_PROXY_PORT, TIMEOUT_PORT_SEC):
            print(f"ERROR: AR at {host}:{HTTP_PROXY_PORT} not ready.")
            print(f"       Check: tail -f {SCREEN_LOG}")
            sys.exit(1)

    print("\n[Phase 3b] Ensuring Docker images are present on RC ...")
    for img in REQUIRED_IMAGES:
        # Skip if already present on RC
        check = subprocess.run(
            ["ssh", CONTROL_PLANE_HOST, f"docker image inspect {img}"],
            capture_output=True
        )
        if check.returncode == 0:
            print(f"   {img}: already on RC, skipping")
            continue

        # Ensure image is present on source AR (pull from Docker Hub if needed)
        src_check = subprocess.run(
            ["ssh", IMAGE_SOURCE_HOST, f"docker image inspect {img}"],
            capture_output=True
        )
        if src_check.returncode != 0:
            print(f"   {img}: pulling on {IMAGE_SOURCE_HOST} ...")
            ret = os.system(f"ssh {IMAGE_SOURCE_HOST} 'docker pull {img}'")
            if ret != 0:
                print(f"   ERROR: failed to pull {img} on {IMAGE_SOURCE_HOST}")
                sys.exit(1)

        print(f"   {img}: transferring from {IMAGE_SOURCE_HOST} to {CONTROL_PLANE_HOST} ...")
        ret = os.system(
            f"ssh {IMAGE_SOURCE_HOST} 'docker save {img}'"
            f" | ssh {CONTROL_PLANE_HOST} 'docker load'"
        )
        if ret != 0:
            print(f"   ERROR: failed to transfer {img} to RC")
            sys.exit(1)
        print(f"   {img}: done")

    print("\n[Phase 4] Launching WordPress service ...")
    cmd = (
        f"XDN_CONTROL_PLANE={CONTROL_PLANE_HOST} {XDN_BINARY} "
        f"launch {SERVICE_NAME} --file={WORDPRESS_YAML}"
    )
    print(f"   {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(f"ERROR: xdn launch failed:\n{result.stderr}")
        sys.exit(1)

    print("\n[Phase 5] Waiting for WordPress HTTP readiness ...")
    ok, _ = wait_for_service(AR_HOSTS, HTTP_PROXY_PORT, SERVICE_NAME, TIMEOUT_WP_SEC)
    if not ok:
        print(f"ERROR: WordPress not ready after {TIMEOUT_WP_SEC}s.")
        sys.exit(1)

    print("\n[Phase 6] Detecting primary ...")
    primary = detect_primary(AR_HOSTS, HTTP_PROXY_PORT, SERVICE_NAME, TIMEOUT_PRIMARY_SEC)
    if not primary:
        print("   WARNING: falling back to AR_HOSTS[0]")
        primary = AR_HOSTS[0]
    print(f"   Primary: {primary}")

    print("\n[Phase 7] Installing WordPress ...")
    if not install_wordpress(primary, HTTP_PROXY_PORT, SERVICE_NAME):
        sys.exit(1)
    if not check_xmlrpc(primary, HTTP_PROXY_PORT, SERVICE_NAME):
        sys.exit(1)

    print("\n[Phase 8] Creating seed posts ...")
    post_ids = []
    for title, content in TEST_POSTS:
        pid = create_post(primary, HTTP_PROXY_PORT, SERVICE_NAME, title, content)
        if pid:
            post_ids.append(pid)
    if not post_ids:
        sys.exit(1)
    if not validate_posts(primary, HTTP_PROXY_PORT, SERVICE_NAME, post_ids):
        sys.exit(1)

    # ── Load test ─────────────────────────────────────────────────────────────
    print("\n[Phase 9] Load-latency sweep ...")
    print(f"   Workload : POST /xmlrpc.php (wp.newPost)  XDN:{SERVICE_NAME}")
    print(f"   Rates    : {LOAD_RATES} req/s")
    print(f"   Duration : {LOAD_DURATION_SEC}s per point")

    results = []
    for rate in LOAD_RATES:
        print(f"\n   -> rate={rate} req/s ...")
        m = run_load_point(primary, rate)
        row = {"rate_rps": rate, **m}
        print(f"     avg={m['avg_ms']:.1f}ms  p50={m['p50_ms']:.1f}ms  "
              f"p95={m['p95_ms']:.1f}ms  tput={m['throughput_rps']:.1f}rps")
        results.append(row)
        if m["p50_ms"] > MEDIAN_LATENCY_THRESHOLD_MS:
            print(f"   Median latency {m['p50_ms']:.0f}ms > threshold, stopping sweep.")
            break

    # ── Output ────────────────────────────────────────────────────────────────
    print("\n[Phase 10] Saving results ...")
    save_csv(results)
    plot_results(results)

    print("\n[Done] Benchmark complete.")
