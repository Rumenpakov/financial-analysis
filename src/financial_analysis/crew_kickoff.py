#!/usr/bin/env python
# src/research_crew/crew_kickoff.py
import os
from src.financial_analysis.crew import ResearchCrew

# Create output directory if it doesn't exist
os.makedirs('output', exist_ok=True)



def run(ticker: str) -> str:
    """
    Run the research crew.
    """
    inputs = {
        'ticker': ticker
    }

    # Create and run the crew
    result = ResearchCrew().crew().kickoff(inputs=inputs)

    # Print the result
    print("\n\n=== FINAL REPORT ===\n\n")
    print(result.raw)

    print("\n\nReport has been saved to output/report.md")

    return result.raw