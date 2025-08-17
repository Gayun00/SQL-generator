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
    """You are a SQL generator agent. You have access to the generate_sql tool.
When asked to create SQL queries, you MUST use the generate_sql tool.
Always call the tool with the query request and schema information.
Do not generate SQL queries directly without using the tool."""
)

# 노드 생성
sql_node = functools.partial(agent_node, agent=sql_agent, name="SQLGenerator")
