const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { Pool } = require('pg');

const PORT = process.env.GRPC_PORT || '50051';
const BIND_ADDRESS = `0.0.0.0:${PORT}`;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || "postgresql://Vukasin:18972@db:5432/iot_db"
});

const packageDefinition = protoLoader.loadSync('./iot.proto', {});
const iotProto = grpc.loadPackageDefinition(packageDefinition).iot;

const insertMeasurement = async (measurement) => {
  const query = {
    text: `
      INSERT INTO measurements (device_id, temperature, humidity, voltage, light_intensity, timestamp)
      VALUES ($1, $2, $3, $4, $5, $6::timestamp)
    `,
    values: [
      measurement.deviceId || "Unknown_Mote",
      measurement.temperature ?? 0.0,
      measurement.humidity ?? 0.0,
      measurement.voltage ?? 0.0,
      measurement.lightIntensity ?? 0.0,
      measurement.timestamp || new Date().toISOString()
    ]
  };

  return pool.query(query);
};

const sendMeasurement = async (call, callback) => {
  try {
    await insertMeasurement(call.request);
    callback(null, { status: "success", message: "Measurement successfully recorded." });
  } catch (err) {
    console.error(`Database insertion failed: ${err.message}`);
    callback({
      code: grpc.status.INTERNAL,
      details: "Internal server error occurred while persisting data."
    });
  }
};

const main = () => {
  const server = new grpc.Server();
  server.addService(iotProto.IoTService.service, { sendMeasurement });

  server.bindAsync(BIND_ADDRESS, grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) {
      console.error(`Failed to bind gRPC server: ${err.message}`);
      process.exit(1);
    }
    console.log(`gRPC server is running on port ${port}`);
    server.start();
  });
};

main();