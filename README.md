# LANdrop

Local network peer-to-peer file and folder sharing.

## Usage

1. Install dependencies and start the server:

   ```bash
   npm install
   node server.js
   ```

   To use a custom port:

   ```bash
   PORT=8080 node server.js
   ```

2. Scan the QR code printed in the terminal (or click **QR Code** in the top bar), or open `http://landrop.local:3000` / the printed LAN IP URL in a browser on each device on the same network.

3. Drag and drop files or a folder (or use **+ Select Folder**), then select a peer to send. Multiple files and folders are bundled into a `.zip` automatically.
