const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { Pool } = require('pg');


const pool = new Pool({
  connectionString: process.env.DATABASE_URL || "postgresql://Vukasin:18972@db:5432/iot_db"
});

const packageDefinition = protoLoader.loadSync('./iot.proto', {});
const iotProto = grpc.loadPackageDefinition(packageDefinition).iot;


const sendMeasurement = async (call, callback) => {
  const m = call.request;
  
  console.log("Podatak iz gRPC-a:", m); 

  const device_id = m.device_id || m.deviceId || "Nepoznato";
  const light = m.light_intensity || m.lightIntensity || 0;

  const query = `
    INSERT INTO measurements (device_id, temperature, humidity, co2, voltage, light_intensity, timestamp)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
  `;
  
  const values = [
    device_id, 
    m.temperature, 
    m.humidity, 
    m.co2, 
    m.voltage, 
    light, 
    m.timestamp
  ];

  try {
    await pool.query(query, values);
    callback(null, { status: "success", message: "Podatak primljen!" });
  } catch (err) {
    console.error("Baza javlja grešku:", err);
    callback(err);
  }
};

const server = new grpc.Server();
server.addService(iotProto.IoTService.service, { sendMeasurement });
server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), () => {
  console.log('gRPC server trči na portu 50051');
  server.start();
});