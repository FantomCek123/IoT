import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    scenarios: {
        // SCENARIO A: k6 naređuje simulatoru da podigne opterećenje na 100, 1000 ili 10000 uređaja
        scenario_A_stress: {
            executor: 'constant-vus',
            vus: 10,
            duration: '15s',
            exec: 'triggerScenarioA',
        },
        // SCENARIO B: k6 paralelno vrši health check-ove na bazu
        scenario_B_monitoring: {
            executor: 'constant-vus',
            vus: 30,
            duration: '30s',
            exec: 'runStorageHealthCheck',
        },
        // SCENARIO C: Burst test - k6 naređuje simulatoru da "grune" 5000 poruka u sekundi
        scenario_C_burst: {
            executor: 'per-vu-iterations',
            vus: 1,
            iterations: 3, // Okinuće burst 3 puta tokom testa
            exec: 'triggerScenarioC',
        },
        // SCENARIO D: Okidanje kritične vrednosti za merenje latencije
        scenario_D_alerting: {
            executor: 'per-vu-iterations',
            vus: 1,
            iterations: 2,
            exec: 'triggerScenarioD',
        }
    },
    thresholds: {
        checks: ['rate>0.95'],
        http_req_duration: ['p(95)<200'], // 95% HTTP health check zahteva mora odgovoriti ispod 200ms
    },
};

const INGESTION_URL = 'http://ingestion_service:3000/run-scenario';
const STORAGE_URL = 'http://storage_service:8000/';

// Okidanje Scenarija A (Promeni num_devices na 100, 1000 ili 10000 u zavisnosti od faze testa)
export function triggerScenarioA() {
    const payload = JSON.stringify({ scenario: 'A', num_devices: 1000 }); 
    http.post(INGESTION_URL, payload, { headers: { 'Content-Type': 'application/json' } });
    sleep(1);
}

// Okidanje Scenarija B (Health monitoring)
export function runStorageHealthCheck() {
    const res = http.get(STORAGE_URL);
    check(res, { 'Storage HTTP je živ (200)': (r) => r.status === 200 });
    sleep(0.5);
}

// Okidanje Scenarija C (Burst)
export function triggerScenarioC() {
    http.post(INGESTION_URL, JSON.stringify({ scenario: 'C' }), { headers: { 'Content-Type': 'application/json' } });
    sleep(10); // Pauza između burst-ova da se sistem oporavi
}

// Okidanje Scenarija D (Kritična vrednost)
export function triggerScenarioD() {
    http.post(INGESTION_URL, JSON.stringify({ scenario: 'D' }), { headers: { 'Content-Type': 'application/json' } });
    sleep(12);
}