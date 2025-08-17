from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

def create_agent(llm: ChatOpenAI, tools: list, system_prompt: str):
    """주어진 LLM, 도구, 프롬프트로 ReAct 에이전트를 생성합니다."""
    # The third argument is the system prompt string, passed with the 'prompt' keyword.
    agent = create_react_agent(llm, tools, prompt=system_prompt)
    return agent

def agent_node(state, agent, name):
    """에이전트를 실행하고 상태를 업데이트하는 노드 함수입니다."""
    result = agent.invoke(state)
    return {"messages": [result["messages"][-1]], "next": name}
