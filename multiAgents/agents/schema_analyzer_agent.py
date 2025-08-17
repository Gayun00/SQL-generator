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
    """You are a schema analyzer agent. You have access to the get_schema_info tool.
When asked about database schemas or tables, you MUST use the get_schema_info tool.
Always call the tool with appropriate parameters to retrieve schema information.
Do not generate responses without using the tool."""
)

# 노드 생성
schema_node = functools.partial(agent_node, agent=schema_agent, name="SchemaAnalyzer")
