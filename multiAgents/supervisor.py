from typing import Literal, TypedDict
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
from multiAgents.config import AGENTS, LLM_MODEL, DEBUG, HUMAN_IN_THE_LOOP
from multiAgents.human_review import simple_human_review

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
    next: Literal["UserCommunicator", "SchemaAnalyzer", "SQLGenerator", "FINISH"]

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
        print(f"🤖 Supervisor decision: Route to {next_worker}")
        if next_worker != "FINISH":
            print(f"Reason: {AGENTS.get(next_worker, {}).get('description', 'Unknown')}")
    
    # FINISH인 경우 바로 반환
    if next_worker == "FINISH":
        return {"next": next_worker}
    
    # Human-in-the-Loop가 활성화된 경우 human review 실행
    if HUMAN_IN_THE_LOOP:
        if not simple_human_review(f"Execute {next_worker}"):
            return {"next": "FINISH"}
    
    # 에이전트 실행
    result_state = execute_agent(next_worker, state)
    
    # 에이전트 실행 후 다음 단계 결정이 필요한지 확인
    # 만약 작업이 완료되었거나 더 이상 진행할 것이 없다면 FINISH
    if should_continue_workflow(result_state):
        # 다음 단계를 위해 Supervisor로 돌아감
        result_state["next"] = "Supervisor"
    else:
        # 작업 완료
        result_state["next"] = "FINISH"
    
    return result_state


def should_continue_workflow(state: dict) -> bool:
    """
    워크플로우를 계속 진행할지 결정합니다.
    
    Args:
        state: 현재 상태
        
    Returns:
        bool: True면 계속 진행, False면 완료
    """
    # 간단한 휴리스틱: 메시지가 있고 에러가 없으면 계속 진행
    messages = state.get("messages", [])
    if not messages:
        return False
    
    # 마지막 메시지가 완료를 나타내는 경우 종료
    last_message = messages[-1] if messages else None
    if last_message and hasattr(last_message, 'content'):
        content = last_message.content.lower()
        if any(keyword in content for keyword in ["complete", "finished", "done", "success"]):
            return False
    
    # 기본적으로는 계속 진행 (나중에 더 정교한 로직 추가 가능)
    return True


def execute_agent(agent_name: str, state: dict) -> dict:
    """
    지정된 에이전트를 실행합니다.
    
    Args:
        agent_name: 실행할 에이전트 이름
        state: 현재 상태
        
    Returns:
        dict: 에이전트 실행 결과가 포함된 상태
    """
    if DEBUG:
        print(f"\n🚀 Executing agent: {agent_name}")
    
    # 각 에이전트별 실행 로직
    if agent_name == "UserCommunicator":
        from multiAgents.agents.user_communicator_agent import user_node
        return user_node(state)
    elif agent_name == "SchemaAnalyzer":
        from multiAgents.agents.schema_analyzer_agent import schema_node
        return schema_node(state)
    elif agent_name == "SQLGenerator":
        from multiAgents.agents.sql_generator_agent import sql_node
        return sql_node(state)
    else:
        if DEBUG:
            print(f"❌ Unknown agent: {agent_name}")
        return state
