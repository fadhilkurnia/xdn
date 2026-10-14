import http from "k6/http";
import { check } from "k6";
import { Trend } from "k6/metrics";

// Point at whichever tier you're testing, e.g.:
//   ADDR=localhost:8000  -> tier 0, direct to container
//   ADDR=localhost:9001  -> tier 1, blocking forwarder
//   ADDR=localhost:9002  -> tier 2a, virtual-thread forwarder
//   ADDR=localhost:9003  -> tier 2b, async forwarder
//   ADDR=localhost:9004  -> tier 3, naive baseline
const ADDR = __ENV.ADDR || "localhost:8000";
const ENDPOINT = "/api/books";
const TIER = __ENV.TIER || "unknown";

const reqLatency = new Trend("req_latency", true);

export const options = {
    scenarios: {
        forward_write: {
            executor: "ramping-arrival-rate",
            startRate: 10,
            timeUnit: "1s",
            preAllocatedVUs: 200,
            maxVUs: 1000,
            stages: [
                { target: 10, duration: "30s" },  // warmup at floor rate
                { target: 500, duration: "2m" },  // ramp 10 -> 500 req/s
                { target: 500, duration: "1m" },  // hold at peak
                { target: 0, duration: "15s" },   // ramp down
            ],
        },
    },
    thresholds: {
        http_req_failed: ["rate<0.01"],
    },
};

export default function () {
    // Date.now() included so read.js and write.js reqIds can't collide if run
    // back-to-back against the same tier process without restarting the container.
    const reqId = `${__VU}-${__ITER}-${Date.now()}`;
    const url = `http://${ADDR}${ENDPOINT}`;
    const payload = JSON.stringify({
        title: `title-${__VU}-${__ITER}`,
        author: `client${__VU}`,
    });

    const res = http.post(url, payload, {
        headers: {
            "Content-Type": "application/json",
            "XDN": "bookcatalog",
            "X-XDN-ReqId": reqId,
        },
    });
    check(res, {
        "status is 2xx": (r) => r.status >= 200 && r.status < 300,
    });
    reqLatency.add(res.timings.duration, { reqId, tier: TIER });
    // No sleep() — open-loop, same reasoning as read.js.
}
