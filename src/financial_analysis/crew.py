# src/research_crew/crew.py
from crewai import Agent, Crew, Process, Task
from crewai.project import CrewBase, agent, crew, task
from crewai.agents.agent_builder.base_agent import BaseAgent
from typing import List

from src.financial_analysis.tools.S3_ticker_news_search_tool import SearchS3ForNewsBasedOnTicker

@CrewBase
class ResearchCrew():
    """Research crew for comprehensive financial analysis and reporting"""

    agents: List[BaseAgent]
    tasks: List[Task]

    @agent
    def stock_news_retriever(self) -> Agent:
        return Agent(
            config=self.agents_config['stock_news_retriever'],
            verbose=True,
            tools=[SearchS3ForNewsBasedOnTicker()]
        )

    @agent
    def stock_news_analyst(self) -> Agent:
        return Agent(
            config=self.agents_config['stock_news_analyst'],
            verbose=True
        )

    @task
    def retriever_task(self) -> Task:
        return Task(
            config=self.tasks_config['retriever_task']
        )

    @task
    def analyst_task(self) -> Task:
        return Task(
            config=self.tasks_config['analyst_task'],
        )

    @crew
    def crew(self) -> Crew:
        return Crew(
            agents=self.agents,
            tasks=self.tasks,
            process=Process.sequential,
            verbose=True,
        )