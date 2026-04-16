"""Entry point — run this to start the FA Rule Converter web app."""
import uvicorn
import os
import sys

# Add backend to path so FastAPI can import convert_fa_rule
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("backend.server:app", host="0.0.0.0", port=port)
