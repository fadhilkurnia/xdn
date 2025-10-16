import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = 'http://10.10.1.1:2300/';
const HEADERS = { 'XDN': 'svc' };

export let options = {
  stages: [
    { target: 10, duration: '1m' },
    { target: 50, duration: '1m' },
    { target: 100, duration: '30s' },
    { target: 200, duration: '30s' },
    { target: 300, duration: '30s' },
    { target: 400, duration: '30s' },
    { target: 500, duration: '30s' },
    { target: 600, duration: '30s' },
    { target: 700, duration: '30s' },
    { target: 800, duration: '30s' },
    { target: 900, duration: '30s' },
    { target: 1000, duration: '30s' },
    { target: 0, duration: '10s' }, // ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<1000'], // 95% of requests should be below 1s
  },
};

export default function () {
  let res = http.get(BASE_URL, { headers: HEADERS });
  check(res, {
    'status is 200': (r) => r.status === 200,
  });
  // minimal sleep to avoid tight loop
  sleep(1 / __ITER);
}
