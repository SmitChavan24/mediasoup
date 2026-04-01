# Mediasoup Application Architecture

This document outlines the detailed system architecture for the Mediasoup-based Two-Way Calling Application.

## System Architecture Diagram

```mermaid
graph TD
    %% Styling Definitions
    classDef frontend fill:#e1f5fe,stroke:#01579b,stroke-width:2px,color:#000;
    classDef backend fill:#f3e5f5,stroke:#4a148c,stroke-width:2px,color:#000;
    classDef mediasoup fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px,color:#000;
    classDef network fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#000;
    classDef note fill:#fff9c4,stroke:#fbc02d,stroke-width:1px,color:#000;

    %% Public Network Layer
    subgraph "Public Internet / Local Network"
        N[Ngrok Reverse Proxy<br/>HTTPS / WSS]:::network
        LAN[Local Wi-Fi Network<br/>WebRTC LAN IPs]:::network
    end

    %% Frontend Clients
    subgraph "Clients (React + Vite PWA)"
        direction LR
        C[Customer PWA<br/>`customer-pwa/src/App.jsx`]:::frontend
        A[Agent PWA<br/>`agent-pwa/src/App.jsx`]:::frontend
        
        MC_C[mediasoup-client<br/>Device & Transports]:::frontend
        MC_A[mediasoup-client<br/>Device & Transports]:::frontend

        C --- MC_C
        A --- MC_A
    end

    %% Backend Infrastructure
    subgraph "Backend Infrastructure (Node.js Environment)"
        direction TB
        EX[Express App<br/>Port 3000]:::backend
        
        subgraph "Signaling Layer (Socket.IO)"
            SIO[Socket Server<br/>`io.on('connection')`]:::backend
            STATE[In-Memory State<br/>`calls`, `activeUsers`]:::backend
            SIO --- STATE
        end
        
        subgraph "Media Layer (Mediasoup)"
            M_NODE[Mediasoup Node Module<br/>`server/mediasoup.js`]:::mediasoup
            M_WORKER[Mediasoup C++ Worker<br/>Subprocess]:::mediasoup
            M_ROUTER[Mediasoup Router<br/>RTP Capabilities]:::mediasoup
            
            M_NODE -->|"Spawns"| M_WORKER
            M_WORKER -->|"Creates"| M_ROUTER
        end
        
        EX --- SIO
        SIO -->|"Configures Transports"| M_NODE
    end

    %% Connections & Data Flows
    C -->|"Signaling (WSS/443)"| N
    A -->|"Signaling (WSS/443)"| N
    N -->|"Forwards (WS/3000)"| EX
    
    %% WebRTC Connections (Direct to Backend)
    MC_C -.->|"WebRTC Media (UDP/TCP)<br/>RTP / RTCP"| LAN
    MC_A -.->|"WebRTC Media (UDP/TCP)<br/>RTP / RTCP"| LAN
    LAN -.->|"Direct Connection<br/>Announced IP"| M_ROUTER

    %% Notes
    class N note;
```

## Call Connection Flow (Sequence Overview)

1. **Authentication & Presence (`Socket.IO`)**
   - Both Customer and Agent connect to the Socket.IO server.
   - The server maintains `activeUsers` state and broadcasts updates via `presenceUpdate` to instantly display active participants in the app roster.

2. **Call Initiation (`Socket.IO`)**
   - The caller clicks "Call", emitting `callIn` or `dialOut`.
   - The server creates a unique `callId` and alerts the target user with `incomingCall`.
   - The target accepts the call, firing `acceptCall`. The initiator receives `callAccepted`.

3. **WebRTC Web Transport Setup (`mediasoup-client` -> `Socket.IO` -> `mediasoup`)**
   - Both clients independently ask the server's router for its `RtpCapabilities`.
   - Clients instruct the server to `createSendTransport` and `createRecvTransport`.
   - The server uses its Mediasoup router to reserve WebRTC listening ports (binding to `getLocalIp()`) and passing the ICE candidates back to the clients.
   - The clients do a localized DTLS handshake (`connectSendTransport` / `connectRecvTransport`).

4. **Media Transmission (`navigator.mediaDevices.getUserMedia` -> `WebRTC`)**
   - Each client queries the browser's audio feed.
   - The `mediasoup-client`'s local `sendTransport` "produces" a Mediasoup component, resulting in ICE media flowing out over UDP/TCP.

5. **Audio Consumption (`Socket.IO` -> `WebRTC`)**
   - The opposing user receives a Socket.IO event `newProducer`.
   - They send a `consume` request to the server, connecting their local `recvTransport` to the opposing audio stream.
   - Using the standard native `<audio>` elements, the incoming byte stream is converted to an audible `MediaStreamTrack`.
