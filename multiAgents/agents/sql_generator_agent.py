import functools
from langchain_openai import ChatOpenAI

from multiAgents.agents.base_agent import create_agent, agent_node
from multiAgents.tools.sql import generate_sql

# LLM 정의
llm = ChatOpenAI(model="gpt-4o")

# 에이전트 생성
sql_agent = create_agent(
    llm,
    [generate_sql],
    "You are a SQL generator. Your role is to create SQL queries based on user questions and schema info."
)

# 노드 생성
sql_node = functools.partial(agent_node, agent=sql_agent, name="SQLGenerator")
