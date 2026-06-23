import grpc from 'k6/net/grpc';
import http from 'k6/http'; 
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';

const grpcClient = new grpc.Client();
grpcClient.load(['./gRPC-service'], 'iot.proto');

const dataset = new SharedArray('intel sensor data', function () {
    return JSON.parse(open('./intel_data.json'));
});

export const options = {
    scenarios: {
        scenario_A_ingestion: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '10s', target: 10 },   
                { duration: '10s', target: 100 },  
                { duration: '20s', target: 500 },  
            ],
            gracefulRampDown: '5s',
            exec: 'runScenarioA',
        },
        scenario_B_monitoring: {
            executor: 'constant-vus',
            vus: 50,                
            duration: '30s',
            exec: 'runScenarioB',
        },
        scenario_C_heavy_queries: {
            executor: 'constant-vus',
            vus: 10,                
            duration: '30s',
            exec: 'runScenarioC',
        },
    },
    thresholds: {
        checks: ['rate>0.99'], 
    },
};

// =============================================================================
// IMPLEMENTACIJA SCENARIJA
// =============================================================================

export function runScenarioA() {
    const randomIndex = Math.floor(Math.random() * dataset.length);
    const data = dataset[randomIndex]; 

   try {
        grpcClient.connect('node_grpc_service:50051', { plaintext: true });
        const grpcResponse = grpcClient.invoke('iot.IoTService/SendMeasurement', data);
        
        if (!grpcResponse || grpcResponse.status !== grpc.StatusOK) {
            console.log(`gRPC Error! Status: ${grpcResponse ? grpcResponse.status : 'No response'}, Message: ${grpcResponse ? grpcResponse.error : 'Unknown'}`);
        }

        check(grpcResponse, { 'gRPC Ingestion OK': (r) => r && r.status === grpc.StatusOK });
    } catch (err) {

    } finally {
        grpcClient.close(); 
    }

    // REST Ingestion
    const restPayload = JSON.stringify(data);
    const restResponse = http.post('http://fastapi_rest_service:8000/measurements/', restPayload, {
        headers: { 'Content-Type': 'application/json' },
    });
    check(restResponse, { 'REST Ingestion OK': (r) => r.status === 201 || r.status === 200 });

    sleep(0.1); 
}

export function runScenarioB() {
    const gqlQuery = JSON.stringify({
        query: `{ allMeasurements { temperature timestamp } }`
    });

    const gqlResponse = http.post('http://fastapi_graphql_service:8001/graphql', gqlQuery, {
        headers: { 'Content-Type': 'application/json' },
    });
    check(gqlResponse, { 'GraphQL Selective OK': (r) => r.status === 200 });

    const restResponse = http.get('http://fastapi_rest_service:8000/measurements/');
    check(restResponse, { 'REST Full Fetch OK': (r) => r.status === 200 });

    sleep(2); 
}

export function runScenarioC() {
    const restAnalytics = http.get('http://fastapi_rest_service:8000/measurements/analytics');
    check(restAnalytics, { 'REST Heavy Analytics OK': (r) => r.status === 200 });

    const gqlAnalyticsQuery = JSON.stringify({
        query: `{ measurementAnalytics { avgTemperature maxHumidity } }`
    });

    const gqlAnalytics = http.post('http://fastapi_graphql_service:8001/graphql', gqlAnalyticsQuery, {
        headers: { 'Content-Type': 'application/json' },
    });
    check(gqlAnalytics, { 'GraphQL Heavy Analytics OK': (r) => r.status === 200 });

    sleep(3); 
}