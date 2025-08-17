from typing import Literal, TypedDict
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, END
from multiAgents.config import AGENTS, LLM_MODEL, DEBUG


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



# Supervisor 내부 그래프 생성
def create_supervisor_graph():
    """
    Supervisor 그래프를 생성합니다. (Router node 제거, 직접 agent 연결)
    
    Returns:
        StateGraph: 컴파일된 supervisor 그래프
    """
    from multiAgents.state import AgentState
    from multiAgents.agents.user_communicator_agent import user_node
    from multiAgents.agents.schema_analyzer_agent import schema_node
    from multiAgents.agents.sql_generator_agent import sql_node
    
    # 그래프 생성
    workflow = StateGraph(AgentState)
    
    # 에이전트 노드들만 추가
    workflow.add_node("UserCommunicator", user_node)
    workflow.add_node("SchemaAnalyzer", schema_node)
    workflow.add_node("SQLGenerator", sql_node)
    
    # 시작점은 supervisor_node에서 동적으로 결정
    workflow.set_entry_point("UserCommunicator")
    
    # 각 에이전트는 작업 완료 후 END로
    workflow.add_edge("UserCommunicator", END)
    workflow.add_edge("SchemaAnalyzer", END)
    workflow.add_edge("SQLGenerator", END)
    
    # 그래프 컴파일
    return workflow.compile()


# Supervisor 그래프 인스턴스 생성
supervisor_graph = create_supervisor_graph()


def supervisor_node(state) -> dict:
    """
    supervisor node입니다. 주어진 State를 기반으로 적절한 worker를 결정하고 실행합니다.

    Args:
        state: Current conversation state with messages
        
    Returns:
        dict: Updated state after running the selected agent, with "next": "FINISH"
    """
    if DEBUG:
        print("\n" + "="*50)
        print("SUPERVISOR ROUTER ANALYSIS")
        print("="*50)
    
    # 메시지 히스토리 구성
    messages = [
        {"role": "system", "content": system_prompt},
    ] + state["messages"]
    
    # LLM을 통해 다음 작업자 결정
    response = llm.with_structured_output(Router).invoke(messages)
    next_worker = response["next"]
    
    if DEBUG:
        print(f"🤖 Router decision: Route to {next_worker}")
        if next_worker != "FINISH":
            print(f"Reason: {AGENTS.get(next_worker, {}).get('description', 'Unknown')}")
    
    # FINISH인 경우 바로 반환
    if next_worker == "FINISH":
        return {**state, "next": "FINISH"}
    
    # 선택된 agent 실행
    try:
        from multiAgents.agents.user_communicator_agent import user_node
        from multiAgents.agents.schema_analyzer_agent import schema_node
        from multiAgents.agents.sql_generator_agent import sql_node
        
        # Agent 매핑
        agent_map = {
            "UserCommunicator": user_node,
            "SchemaAnalyzer": schema_node,
            "SQLGenerator": sql_node
        }
        
        if next_worker in agent_map:
            if DEBUG:
                print(f"🚀 Executing {next_worker}")
            
            # 선택된 agent 실행
            result = agent_map[next_worker](state)
            
            if DEBUG:
                print(f"✅ {next_worker} completed")
            
            return {**result, "next": "FINISH"}
        else:
            if DEBUG:
                print(f"❌ Unknown worker: {next_worker}")
            return {**state, "next": "FINISH"}
            
    except Exception as e:
        if DEBUG:
            print(f"❌ Error executing {next_worker}: {str(e)}")
        return {**state, "next": "FINISH"}
