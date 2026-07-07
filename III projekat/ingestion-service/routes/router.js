const simService = require('../services/simulation');

function handleRoutes(req, res) {
    // ROUTE 1: Start
    if (req.method === 'POST' && req.url === '/start-simulation') {
        let body = '';
        req.on('data', chunk => { body += chunk.toString(); });
        req.on('end', () => {
            const config = JSON.parse(body);
            simService.startRealTimeSimulation(config.num_devices || 50);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            return res.end(JSON.stringify({ status: "Simulacija pokrenuta." }));
        });
    }
    // ROUTE 2: Stop
    else if (req.method === 'POST' && req.url === '/stop-simulation') {
        const stopped = simService.stopRealTimeSimulation();
        if (stopped) {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            return res.end(JSON.stringify({ status: "Simulacija zaustavljena." }));
        } else {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            return res.end(JSON.stringify({ status: "Nema aktivne simulacije." }));
        }
    }
    // ROUTE 3: Alarm
    else if (req.method === 'POST' && req.url === '/trigger-alarm') {
        simService.runCriticalAlarmStream();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ status: "Strim visokih temperatura pokrenut." }));
    } 
    // Fallback
    else {
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        return res.end('Not Found');
    }
}

module.exports = { handleRoutes };