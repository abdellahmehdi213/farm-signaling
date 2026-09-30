/**
 * Lightweight WebRTC Signaling Server for Farm Camera & Farm Monitor
 * Protocol: WebSocket (JSON)
 * Port: 8443 (default)
 *
 * Run with: node signaling_server.js
 */

const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8443;

// Map: cameraId -> { ws, token, role, metadata }
const cameras = new Map();
// Map: ws -> { cameraId, role }
const clients = new Map();

const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    const camList = [];
    for (const [id, cam] of cameras.entries()) {
        camList.push({ id, token: cam.token ? cam.token.substring(0, 8) + '...' : '', role: cam.role });
    }
    const clientList = [];
    for (const [ws, info] of clients.entries()) {
        clientList.push(info);
    }
    res.end(JSON.stringify({ 
        status: 'Farm Camera Signaling Server Active', 
        timestamp: Date.now(),
        camerasCount: cameras.size,
        cameras: camList,
        clientsCount: clients.size,
        clients: clientList
    }, null, 2));
});

const wss = new WebSocket.Server({ server });


wss.on('connection', (ws) => {
    console.log('[+] New WebSocket connection established');

    ws.on('message', (message, isBinary) => {
        // High-speed binary video streaming relay (Camera -> Viewer)
        const firstByte = Buffer.isBuffer(message) ? message[0] : (typeof message === 'string' ? message.charCodeAt(0) : 0);
        const isJson = firstByte === 0x7B || firstByte === 0x5B; // '{' or '['
        
        if (!isJson || isBinary) {
            const client = clients.get(ws);
            if (client && client.role === 'camera') {
                const targetCameraId = client.cameraId;
                wss.clients.forEach((c) => {
                    if (c !== ws && c.readyState === WebSocket.OPEN) {
                        const info = clients.get(c);
                        if (info && info.cameraId === targetCameraId && info.role === 'viewer') {
                            c.send(message, { binary: true });
                        }
                    }
                });
            }
            return;
        }

        try {
            const data = JSON.parse(message.toString());
            const action = data.action;

            switch (action) {
                case 'register': {
                    // Registration from Camera or Viewer
                    const cameraId = data.cameraId;
                    const token = data.token;
                    const role = data.role; // 'camera' or 'viewer'

                    if (!cameraId) {
                        ws.send(JSON.stringify({ action: 'error', message: 'Missing cameraId' }));
                        return;
                    }

                    if (role === 'camera') {
                        cameras.set(cameraId, { ws, token, role: 'camera' });
                        clients.set(ws, { cameraId, role: 'camera' });
                        console.log(`[Camera Registered] ID: ${cameraId}`);
                        ws.send(JSON.stringify({ action: 'registered', cameraId, role: 'camera' }));
                    } else if (role === 'viewer') {
                        // Viewer authenticates using Camera ID + Token
                        const registeredCam = cameras.get(cameraId);
                        if (!registeredCam) {
                            ws.send(JSON.stringify({ action: 'error', message: 'Camera currently offline' }));
                            return;
                        }
                        if (registeredCam.token !== token) {
                            ws.send(JSON.stringify({ action: 'error', message: 'Invalid authentication token' }));
                            return;
                        }
                        clients.set(ws, { cameraId, role: 'viewer' });
                        console.log(`[Viewer Connected] to Camera ID: ${cameraId}`);
                        ws.send(JSON.stringify({ action: 'registered', cameraId, role: 'viewer' }));
                    }
                    break;
                }

                case 'request_stream': {
                    // Viewer requests camera to start streaming
                    const client = clients.get(ws);
                    if (client && client.role === 'viewer') {
                        const cam = cameras.get(client.cameraId);
                        if (cam && cam.ws.readyState === WebSocket.OPEN) {
                            cam.ws.send(JSON.stringify({ action: 'request_stream', senderId: 'viewer' }));
                        }
                    }
                    break;
                }

                case 'offer': {
                    // Relay SDP Offer from Camera to Viewer
                    const client = clients.get(ws);
                    if (client) {
                        relayToPeer(client, ws, data);
                    }
                    break;
                }

                case 'answer': {
                    // Relay SDP Answer from Viewer to Camera
                    const client = clients.get(ws);
                    if (client) {
                        relayToPeer(client, ws, data);
                    }
                    break;
                }

                case 'ice_candidate': {
                    // Relay ICE Candidates between peers
                    const client = clients.get(ws);
                    if (client) {
                        relayToPeer(client, ws, data);
                    }
                    break;
                }

                case 'stop_stream': {
                    const client = clients.get(ws);
                    if (client) {
                        relayToPeer(client, ws, data);
                    }
                    break;
                }

                case 'ping': {
                    ws.send(JSON.stringify({ action: 'pong', timestamp: Date.now() }));
                    break;
                }

                case 'motion_alert': {
                    const client = clients.get(ws);
                    if (client) {
                        console.log(`[Motion Alert] Relay from ${client.role} for camera ${client.cameraId}`);
                        relayToPeer(client, ws, data);
                    }
                    break;
                }

                default: {
                    const client = clients.get(ws);
                    if (client) {
                        relayToPeer(client, ws, data);
                    }
                    console.log(`[?] Relayed message action: ${action}`);
                }
            }
        } catch (err) {
            console.error('[!] Error handling message:', err.message);
        }
    });

    ws.on('close', () => {
        const client = clients.get(ws);
        if (client) {
            console.log(`[-] Client disconnected: ${client.role} for camera ${client.cameraId}`);
            if (client.role === 'camera') {
                cameras.delete(client.cameraId);
            }
            clients.delete(ws);
        }
    });
});

function relayToPeer(senderClient, senderWs, payload) {
    const targetCameraId = senderClient.cameraId;
    wss.clients.forEach((client) => {
        if (client !== senderWs && client.readyState === WebSocket.OPEN) {
            const info = clients.get(client);
            if (info && info.cameraId === targetCameraId) {
                client.send(JSON.stringify(payload));
            }
        }
    });
}

server.listen(PORT, () => {
    console.log(`===============================================`);
    console.log(`  Farm Camera Signaling Server running on :${PORT}`);
    console.log(`===============================================`);
});
