from typing import Literal, TypedDict
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
from multiAgents.config import AGENTS, LLM_MODEL, DEBUG, HUMAN_IN_THE_LOOP

# LLM 정의
llm = ChatOpenAI(model=LLM_MODEL)

# 사용 가능한 에이전트 목록
members = list(AGENTS.keys())
options = members + ["FINISH"]

# 에이전트 설명 생성
agent_descriptions = "\n".join([
    f"- {name}: {info['description']}"
    for name, info in AGENTS.items()
])

# Supervisor 시스템 프롬프트
system_prompt = (
    "You are a supervisor tasked with managing a conversation between the "
    f"following workers: {', '.join(members)}.\n\n"
    "Worker Capabilities:\n"
    f"{agent_descriptions}\n\n"
    "Instructions:\n"
    "1. Analyze the user's request and conversation history\n"
    "2. Determine which worker should handle the next step\n"
    "3. Consider the natural flow of tasks (e.g., get schema before generating SQL)\n"
    "4. When the user's request has been fully addressed, respond with FINISH\n\n"
    "Make your decision based on what needs to be done next, not on explicit rules."
)

class Router(TypedDict):
    """Worker to route to next. If no workers needed, route to FINISH."""
    next: Literal["SchemaAnalyzer", "SQLGenerator", "FINISH"]

def supervisor_node(state) -> dict:
    """
    Supervisor node that manages the workflow between workers.
    
    Args:
        state: Current conversation state with messages
        
    Returns:
        dict: Contains the next worker to execute or FINISH
    """
    if DEBUG:
        print("\n" + "="*50)
        print("SUPERVISOR ANALYSIS")
        print("="*50)
    
    # 메시지 히스토리 구성
    messages = [{"role": "system", "content": system_prompt}]
    
    # 상태에서 메시지 추출 및 포맷팅
    for msg in state["messages"]:
        if hasattr(msg, '__class__'):
            msg_type = msg.__class__.__name__
            if msg_type == "HumanMessage":
                messages.append({"role": "user", "content": msg.content})
            elif msg_type == "AIMessage":
                messages.append({"role": "assistant", "content": msg.content if msg.content else "Tool executed"})
            elif msg_type == "ToolMessage":
                messages.append({"role": "assistant", "content": f"Tool result: {msg.content}"})
    
    # LLM을 통해 다음 작업자 결정
    response = llm.with_structured_output(Router).invoke(messages)
    next_worker = response["next"]
    
    if DEBUG:
        print(f"Decision: Route to {next_worker}")
        if next_worker != "FINISH":
            print(f"Reason: {AGENTS.get(next_worker, {}).get('description', 'Unknown')}")
    
    return {"next": next_worker}
