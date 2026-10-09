# Case Club Test

Public trainer interface and an authenticated connection to the author's private trainer server.

Only compiled browser assets and the gateway are stored here. The GigaChat key, grading prompts, private answers, local databases, configuration, and source case book are not included.

Node.js 22.13+; `npm ci`, then `npm start`. Runtime variables: `BRIDGE_TOKEN` (a private random 32-byte base64url value), `PUBLIC_ORIGIN` (an exact HTTPS origin; Render supplies `RENDER_EXTERNAL_URL`), `PORT` (Render supplies this).

Deploy a single `free` web service. The gateway does not use local database storage: progress and AI usage limits stay in the author's persistent private database. If that server is disconnected, the page stays available and the API reports that it is offline. No synthetic grades are generated.

The Free service may sleep, restart, or be unavailable. A cold start can take about a minute. No paid services or permanent cloud accounts are implied.
