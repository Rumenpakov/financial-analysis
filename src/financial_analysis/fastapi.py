import json
import os
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from src.financial_analysis.crew_kickoff import run

# Load environment variables from a .env file if present (useful for local dev)
# In Docker, OPENAI_API_KEY should be passed via -e OPENAI_API_KEY=...
load_dotenv()

app = FastAPI()


@app.get("/analyze/{ticker}")
async def analyze(ticker: str):
    try:
        raw = run(ticker)
        # Ensure we return proper JSON. tasks.yaml enforces JSON output, so try to parse it.
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            # If the model returned non-JSON, surface a clear error for debugging.
            raise HTTPException(status_code=500, detail="Model did not return valid JSON.")
        return JSONResponse(content=data)
    except HTTPException:
        # Re-raise HTTPExceptions directly
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
