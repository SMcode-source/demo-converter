# FA Rule Converter — Web Demo

Converts actuarial Fine Adjustment rule files (Excel) into flat CSV tables.

## Quick Start

```bash
pip install -r requirements.txt
python run_server.py
```

Open http://localhost:8080 in your browser. Password: set via `APP_PASSWORD` environment variable.

## Deploy to Render

1. Push this repo to GitHub
2. Create a new Web Service on [render.com](https://render.com)
3. Connect the GitHub repo
4. Render will auto-detect `render.yaml` and deploy

## Tech Stack

- **Backend**: FastAPI + Python (wraps convert_fa_rule.py)
- **Frontend**: React (pre-built, served as static files)
- **Progress**: Server-Sent Events (SSE)
