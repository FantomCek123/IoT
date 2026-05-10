import grpc from 'k6/net/grpc';
import http from 'k6/http'; // MORAMO dodati http modul
import { check, sleep } from 'k6';

const client = new grpc.Client();
client.load(['/'], 'iot.proto');

export let options = {
    stages: [
        { duration: '10s', target: 10 },  
        { duration: '10s', target: 100 }, 
        { duration: '20s', target: 500 }, 
    ],
};

export default function () {
    // --- 1. TEST REST (FastAPI) ---
    let resRest = http.get('http://fastapi_rest_service:8000/measurements/');
    check(resRest, { 'REST status 200': (r) => r.status === 200 });

    // --- 2. TEST GRAPHQL (FastAPI) ---
    const gqlQuery = JSON.stringify({
        query: `{ allMeasurements { temperature timestamp } }`
    });
    let resGql = http.post('http://fastapi_graphql_service:8000/graphql', gqlQuery, {
        headers: { 'Content-Type': 'application/json' },
    });
    check(resGql, { 'GraphQL status 200': (r) => r.status === 200 });

    // --- 3. TEST gRPC (Node.js) ---
    client.connect('node_grpc_service:50051', { plaintext: true });
    
    const data = {
        device_id: "sensor-01",
        temperature: 24.5,
        humidity: 50.2,
        co2: 400.0,
        voltage: 12.0,
        light_intensity: 300.0,
        timestamp: new Date().toISOString()
    };

    const response = client.invoke('iot.IoTService/SendMeasurement', data);
    check(response, { 'gRPC status is OK': (r) => r && r.status === grpc.StatusOK });

    client.close();
    

    sleep(0.1);
}