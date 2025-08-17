from typing import Literal, TypedDict
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from multiAgents.config import AGENTS, LLM_MODEL, DEBUG, HUMAN_IN_THE_LOOP
from multiAgents.human_review import simple_human_review, human_review_node

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

def router_node(state) -> dict:
    """
    Router node that determines which worker should handle the next step.
    
    Args:
        state: Current conversation state with messages
        
    Returns:
        dict: Contains the next worker to execute or FINISH
    """
    if DEBUG:
        print("\n" + "="*50)
        print("SUPERVISOR ROUTER ANALYSIS")
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
        print(f"🤖 Router decision: Route to {next_worker}")
        if next_worker != "FINISH":
            print(f"Reason: {AGENTS.get(next_worker, {}).get('description', 'Unknown')}")
    
    return {"next": next_worker}


# Supervisor 내부 그래프 생성
def create_supervisor_graph():
    """
    Supervisor 내부 그래프를 생성합니다.
    
    Returns:
        StateGraph: 컴파일된 supervisor 그래프
    """
    from multiAgents.state import AgentState
    from multiAgents.agents.user_communicator_agent import user_node
    from multiAgents.agents.schema_analyzer_agent import schema_node
    from multiAgents.agents.sql_generator_agent import sql_node
    
    # 그래프 생성
    workflow = StateGraph(AgentState)
    
    # Router 노드 추가 (다음 에이전트 결정)
    workflow.add_node("Router", router_node)
    
    # 에이전트 노드들 추가
    workflow.add_node("UserCommunicator", user_node)
    workflow.add_node("SchemaAnalyzer", schema_node)
    workflow.add_node("SQLGenerator", sql_node)
    
    # Human-in-the-Loop가 활성화된 경우 HumanReview 노드 추가
    if HUMAN_IN_THE_LOOP:
        workflow.add_node("HumanReview", human_review_node)
    
    # 시작점 설정
    workflow.set_entry_point("Router")
    
    # Router에서 다음 노드로의 conditional edges
    if HUMAN_IN_THE_LOOP:
        # Human Review를 거쳐서 에이전트로 가는 구조
        def router_decision(x):
            next_step = x.get("next", "FINISH")
            if DEBUG:
                print(f"🔀 Router decision function called with: {x}")
                print(f"🔀 Next step extracted: {next_step}")
            
            if next_step in ["UserCommunicator", "SchemaAnalyzer", "SQLGenerator"]:
                if DEBUG:
                    print(f"🔀 Routing to HumanReview for: {next_step}")
                return "HumanReview"
            else:
                if DEBUG:
                    print(f"🔀 Routing to FINISH")
                return "FINISH"
        
        workflow.add_conditional_edges(
            "Router",
            router_decision,
            {"HumanReview": "HumanReview", "FINISH": END}
        )
        
        # HumanReview에서 에이전트로
        workflow.add_conditional_edges(
            "HumanReview",
            lambda x: x["next"],
            {
                "UserCommunicator": "UserCommunicator",
                "SchemaAnalyzer": "SchemaAnalyzer", 
                "SQLGenerator": "SQLGenerator",
                "FINISH": END
            }
        )
    else:
        # 직접 에이전트로 가는 구조
        workflow.add_conditional_edges(
            "Router",
            lambda x: x["next"],
            {
                "UserCommunicator": "UserCommunicator",
                "SchemaAnalyzer": "SchemaAnalyzer",
                "SQLGenerator": "SQLGenerator",
                "FINISH": END
            }
        )
    
    # 각 에이전트에서 Router로 돌아가는 edge
    workflow.add_edge("UserCommunicator", "Router")
    workflow.add_edge("SchemaAnalyzer", "Router")
    workflow.add_edge("SQLGenerator", "Router")
    
    # 그래프 컴파일
    if HUMAN_IN_THE_LOOP:
        memory = MemorySaver()
        return workflow.compile(checkpointer=memory, interrupt_before=["HumanReview"])
    else:
        return workflow.compile()


# Supervisor 그래프 인스턴스 생성
supervisor_graph = create_supervisor_graph()


def supervisor_node(state) -> dict:
    """
    Supervisor node that manages the internal graph workflow.
    
    Args:
        state: Current conversation state with messages
        
    Returns:
        dict: Updated state after running the supervisor graph
    """
    if DEBUG:
        print("\n" + "="*50)
        print("SUPERVISOR STARTING INTERNAL GRAPH")
        print("="*50)
    
    try:
        if HUMAN_IN_THE_LOOP:
            from langgraph.errors import GraphInterrupt
            from multiAgents.human_review import simple_human_review
            
            # Human-in-the-loop가 활성화된 경우 중단 처리
            config = {"configurable": {"thread_id": "default"}}
            
            try:
                # 첫 번째 실행 (중단될 수 있음)
                result = supervisor_graph.invoke(state, config)
                if DEBUG:
                    print("="*50)
                    print("SUPERVISOR INTERNAL GRAPH COMPLETED")
                    print("="*50)
                return {**result, "next": "FINISH"}
                
            except GraphInterrupt:
                # 중단된 경우 현재 상태 확인
                current_state = supervisor_graph.get_state(config)
                next_worker = current_state.values.get("next", "FINISH")
                
                if DEBUG:
                    print(f"🚦 Graph interrupted. Next worker: {next_worker}")
                
                # Human review 수행
                if next_worker != "FINISH":
                    if simple_human_review(f"Route to {next_worker}"):
                        # 승인된 경우 계속 진행
                        result = supervisor_graph.invoke(None, config)
                        if DEBUG:
                            print("="*50)
                            print("SUPERVISOR INTERNAL GRAPH COMPLETED")
                            print("="*50)
                        return {**result, "next": "FINISH"}
                    else:
                        # 거부된 경우 종료
                        return {**state, "next": "FINISH"}
                else:
                    return {**state, "next": "FINISH"}
        else:
            # Human-in-the-loop가 비활성화된 경우 일반 실행
            result = supervisor_graph.invoke(state)
            
            if DEBUG:
                print("="*50)
                print("SUPERVISOR INTERNAL GRAPH COMPLETED")
                print("="*50)
            
            return {**result, "next": "FINISH"}
        
    except Exception as e:
        if DEBUG:
            print(f"❌ Supervisor Graph Error: {type(e).__name__}: {str(e)}")
            import traceback
            traceback.print_exc()
        
        # 에러 발생 시 FINISH로 설정
        return {**state, "next": "FINISH"}
