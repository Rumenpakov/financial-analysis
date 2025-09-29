from crewai.tools import BaseTool
from typing import Type
from pydantic import BaseModel, Field


class Ticker(BaseModel):
    ticker: str = Field(..., description="Ticker symbol to search article news for.")


class SearchS3ForNewsBasedOnTicker(BaseTool):
    name: str = "S3 Ticker News Data Lake"
    description: str = (
        "This tool searches for article news based on provided ticker symbol."
    )
    args_schema: Type[BaseModel] = Ticker

    def _run(self, ticker: str) -> str:
        # Implementation goes here
        return f"{ticker} surges 5000%."
