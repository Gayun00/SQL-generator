from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import SystemMessage

def create_agent(llm: ChatOpenAI, tools: list, system_prompt: str):
    """주어진 LLM, 도구, 프롬프트로 ReAct 에이전트를 생성합니다."""
    
    # SystemMessage 또는 문자열로 프롬프트 전달
    agent = create_react_agent(llm, tools, prompt=SystemMessage(content=system_prompt))
    return agent

def agent_node(state, agent, name):
    """에이전트를 실행하고 상태를 업데이트하는 노드 함수입니다."""
    # 에이전트 실행 (create_react_agent는 완전한 그래프를 반환하므로 invoke 사용)
    result = agent.invoke({"messages": state["messages"]})
    
    # 모든 새로운 메시지를 반환 (tool calls와 tool responses 포함)
    # 입력 메시지를 제외한 새로운 메시지들만 반환
    original_len = len(state["messages"])
    new_messages = result["messages"][original_len:]
    
    return {"messages": new_messages}
