import functools
from langchain_openai import ChatOpenAI

from multiAgents.agents.base_agent import create_agent, agent_node
from multiAgents.tools.schema import get_schema_info

# LLM 정의
llm = ChatOpenAI(model="gpt-4o")

# 에이전트 생성
schema_agent = create_agent(
    llm,
    [get_schema_info],
    "You are a schema analyzer. Your role is to get database schema information."
)

# 노드 생성
schema_node = functools.partial(agent_node, agent=schema_agent, name="SchemaAnalyzer")
