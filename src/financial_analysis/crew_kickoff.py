from src.financial_analysis.crew import ResearchCrew

def run(ticker: str) -> str:
    inputs = {
        'ticker': ticker
    }

    result = ResearchCrew().crew().kickoff(inputs=inputs)

    print("\n\n=== FINAL REPORT ===\n\n")
    print(result.raw)
    print("\n\nReport has been saved to output/report.md")

    return result.raw