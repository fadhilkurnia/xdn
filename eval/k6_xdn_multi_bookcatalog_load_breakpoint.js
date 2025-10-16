// Example command to run this breakpoint test:
//  K6_PROMETHEUS_RW_SERVER_URL=http://localhost:9090/api/v1/write \
//  K6_PROMETHEUS_RW_TREND_STATS='p(90),p(95),p(99),max,avg,med' \
//  k6 run -o experimental-prometheus-rw --tag testid=breakpoint-multi-1 \
//     k6_xdn_multi_bookcatalog_load_breakpoint.js

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Counter, Trend } from 'k6/metrics';

const BASE_URL = 'http://10.10.1.1:2300/api/books';
const SERVICES = [
    'bookcatalog1',
    'bookcatalog2',
    'bookcatalog3',
    'bookcatalog4',
    'bookcatalog5',
    'bookcatalog6',
    'bookcatalog7',
    'bookcatalog8'
];

// Create metrics for each service
const errorRates = {};
const responseTimeTrends = {};
const requestCounters = {};

SERVICES.forEach(service => {
    errorRates[service] = new Rate(`errors_${service}`);
    responseTimeTrends[service] = new Trend(`response_time_${service}`);
    requestCounters[service] = new Counter(`requests_${service}`);
});

export let options = {
    stages: [
        { target: 10000, duration: '30m' },
        { target: 10000, duration: '1m' },
    ],
    thresholds: {
        'http_req_duration': ['p(95)<1000'], // 95% of requests should be below 1s
    },
};

export default function () {
    // Randomly select one of the services
    const serviceIndex = Math.floor(Math.random() * SERVICES.length);
    const service = SERVICES[serviceIndex];
    
    // Make request with the selected service header
    const res = http.get(BASE_URL, { 
        headers: { 'XDN': service }
    });
    
    // Check response
    check(res, {
        [`status is 200 for ${service}`]: (r) => r.status === 200,
    });

    // Track custom metrics for the specific service
    errorRates[service].add(res.status !== 200);
    responseTimeTrends[service].add(res.timings.duration);
    requestCounters[service].add(1);

    sleep(0.5); // pause for 0.5 second between requests
}

// Helper function to create service-specific metric names
function getMetricName(service, metricType) {
    return `${service}_${metricType}`;
}
