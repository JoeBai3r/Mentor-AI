# Activity Monitor

Monorepo with a Rust background daemon (daemon/) and a React + Tauri frontend (frontend/).

- daemon/: Rust binary that runs as a background service to monitor activity, exposing a REST/WebSocket API on `localhost:3030`.
- frontend/: React app (Vite) wrapped in a Tauri desktop shell. The desktop app launches the daemon as a managed sidecar process and talks to it over `localhost:3030`.

## Quick start (desktop app)

```
./scripts/start-app.sh
```

This builds the daemon, copies it into `frontend/src-tauri/binaries/` as the Tauri sidecar, and launches the app via `tauri dev` (daemon + UI together, with live frontend reload).

If you only changed daemon code and the app is already running, rebuild the sidecar with:

```
./scripts/build-daemon-sidecar.sh
```

then restart the app.

See README in each subproject for more details.
