# Steam Signal Dashboard

React + TypeScript dashboard for the read-only Steam Backend API.

## Run locally

Run these commands from the repository root. Start MongoDB and ensure the historical serving collections have been loaded. In one terminal, start the API:

```powershell
Set-Location backend-api
python -m pip install -r requirements.txt
python -m uvicorn app.main:app --reload --port 8000
```

In another terminal, start the dashboard:

```powershell
Set-Location frontend
npm install
npm run dev
```

Open `http://localhost:5173`. The default API URL is `http://localhost:8000`; override it with `VITE_API_BASE_URL` in `.env.local` when needed. The backend must allow the frontend origin through `CORS_ORIGINS`.

The overview and game catalog use historical analytics collections. The Live feed reads incremental reviews; an empty feed is expected until the realtime serving pipeline has received new events.

## Checks

```powershell
npm run lint
npm run build
```
