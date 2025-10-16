// Example command to run this breakpoint test:
//  K6_PROMETHEUS_RW_SERVER_URL=http://localhost:9090/api/v1/write \
//  K6_PROMETHEUS_RW_TREND_STATS='p(90),p(95),p(99),max,avg,med' \
//  k6 run -o experimental-prometheus-rw --tag testid=breakpoint-1 \
//     k6_docker_bookcatalog_load_breakpoint.js

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Counter, Trend } from 'k6/metrics';

const BASE_URL = 'http://10.10.1.1:2300/api/books';
const HEADERS = { 'XDN': 'bookcatalog' };

export const errorRate = new Rate('errors');           // Tracks percentage of errors
export const myCounter = new Counter('my_counter');    // Simple incrementing counter
export const responseTime = new Trend('response_time'); // Tracks response time distribution


export let options = {
  stages: [
    { target: 10000, duration: '30m' },
    { target: 10000, duration: '1m' },
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

  // track custom metrics
  errorRate.add(res.status !== 200);
  responseTime.add(res.timings.duration);
  myCounter.add(1);

  sleep(0.5); // pause for 0.5 second between requests
}