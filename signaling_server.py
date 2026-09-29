"""
Lightweight WebRTC Signaling Server for Farm Camera & Farm Monitor (Python 3.9+)
Requirements: pip install websockets
Run: python signaling_server.py
"""

import asyncio
import json
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

PORT = int(os.environ.get("PORT", 8443))

# Map: camera_id -> {"ws": ws, "token": token}
cameras = {}
# Map: ws -> {"camera_id": id, "role": role}
clients = {}

async def relay_to_peer(sender_ws, camera_id, message_str):
    for ws, client_info in clients.items():
        if ws != sender_ws and client_info.get("camera_id") == camera_id:
            try:
                await ws.send(message_str)
            except Exception as e:
                logging.error(f"Error relaying to peer: {e}")

async def handler(websocket):
    logging.info("[+] New WebSocket connection established")
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                action = data.get("action")

                if action == "register":
                    camera_id = data.get("cameraId")
                    token = data.get("token")
                    role = data.get("role", "camera")

                    if not camera_id:
                        await websocket.send(json.dumps({"action": "error", "message": "Missing cameraId"}))
                        continue

                    if role == "camera":
                        cameras[camera_id] = {"ws": websocket, "token": token}
                        clients[websocket] = {"camera_id": camera_id, "role": "camera"}
                        logging.info(f"[Camera Registered] ID: {camera_id}")
                        await websocket.send(json.dumps({"action": "registered", "cameraId": camera_id, "role": "camera"}))

                    elif role == "viewer":
                        registered_cam = cameras.get(camera_id)
                        if not registered_cam:
                            await websocket.send(json.dumps({"action": "error", "message": "Camera currently offline"}))
                            continue
                        if registered_cam["token"] != token:
                            await websocket.send(json.dumps({"action": "error", "message": "Invalid authentication token"}))
                            continue

                        clients[websocket] = {"camera_id": camera_id, "role": "viewer"}
                        logging.info(f"[Viewer Connected] to Camera ID: {camera_id}")
                        await websocket.send(json.dumps({"action": "registered", "cameraId": camera_id, "role": "viewer"}))

                elif action == "request_stream":
                    client_info = clients.get(websocket)
                    if client_info and client_info["role"] == "viewer":
                        cam = cameras.get(client_info["camera_id"])
                        if cam and cam["ws"]:
                            await cam["ws"].send(json.dumps({"action": "request_stream", "senderId": "viewer"}))

                elif action in ("offer", "answer", "ice_candidate", "stop_stream"):
                    client_info = clients.get(websocket)
                    if client_info:
                        await relay_to_peer(websocket, client_info["camera_id"], message)

                elif action == "ping":
                    await websocket.send(json.dumps({"action": "pong"}))

            except json.JSONDecodeError:
                logging.warning("[!] Invalid JSON received")

    except Exception as e:
        logging.info(f"[-] Connection terminated: {e}")
    finally:
        client_info = clients.pop(websocket, None)
        if client_info:
            camera_id = client_info["camera_id"]
            if client_info["role"] == "camera":
                cameras.pop(camera_id, None)
                logging.info(f"[Camera Disconnected] ID: {camera_id}")
            else:
                logging.info(f"[Viewer Disconnected] from Camera ID: {camera_id}")

async def main():
    import websockets
    logging.info(f"Starting Farm Camera Signaling Server on port {PORT}...")
    async with websockets.serve(handler, "0.0.0.0", PORT):
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Server stopped.")
