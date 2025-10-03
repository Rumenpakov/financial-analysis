import json
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from src.financial_analysis.crew_kickoff import run

load_dotenv()

app = FastAPI()


@app.get("/analyze/{ticker}")
async def analyze(ticker: str):
    try:
        raw: str = run(ticker).removeprefix("```json").removesuffix("```")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            raise HTTPException(status_code=500, detail="Model did not return valid JSON.")
        return JSONResponse(content=data)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def read_root():
    return {"message": "Server is running"}