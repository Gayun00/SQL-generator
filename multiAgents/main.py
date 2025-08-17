import os
from dotenv import load_dotenv
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.messages import HumanMessage

# .env 파일에서 환경 변수 로드
load_dotenv()

# LangSmith 추적을 위한 환경 변수 설정 (선택 사항)
# os.environ["LANGCHAIN_TRACING_V2"] = "true"
# os.environ["LANGCHAIN_API_KEY"] = "YOUR_LANGCHAIN_API_KEY"

from multiAgents.state import AgentState
from multiAgents.supervisor import supervisor_node
from multiAgents.agents.schema_analyzer_agent import schema_node
from multiAgents.agents.sql_generator_agent import sql_node
from multiAgents.human_review import human_review_node
from multiAgents.config import AGENTS, DEFAULT_RECURSION_LIMIT, DEBUG, HUMAN_IN_THE_LOOP

# --- 그래프 생성 ---
workflow = StateGraph(AgentState)

# 노드 추가
workflow.add_node("Supervisor", supervisor_node)
workflow.add_node("SchemaAnalyzer", schema_node)
workflow.add_node("SQLGenerator", sql_node)

# Human-in-the-Loop가 활성화된 경우 HumanReview 노드 추가
if HUMAN_IN_THE_LOOP:
    workflow.add_node("HumanReview", human_review_node)

# 엣지(연결) 정의
if HUMAN_IN_THE_LOOP:
    # Human-in-the-Loop 모드: Supervisor → HumanReview → Agent/FINISH
    workflow.add_conditional_edges(
        "Supervisor",
        lambda x: "HumanReview" if x["next"] != "FINISH" else "FINISH",
        {"HumanReview": "HumanReview", "FINISH": END}
    )
    
    # HumanReview에서 Agent 또는 Supervisor 또는 FINISH로
    review_edge_mapping = {agent: agent for agent in AGENTS.keys()}
    review_edge_mapping["Supervisor"] = "Supervisor"
    review_edge_mapping["FINISH"] = END
    
    workflow.add_conditional_edges(
        "HumanReview",
        lambda x: x["next"],
        review_edge_mapping
    )
    
    # 각 에이전트에서 다시 Supervisor로
    for agent_name in AGENTS.keys():
        workflow.add_edge(agent_name, "Supervisor")
        
else:
    # 기본 모드: Supervisor → Agent 직접 연결
    edge_mapping = {agent: agent for agent in AGENTS.keys()}
    edge_mapping["FINISH"] = END
    
    workflow.add_conditional_edges(
        "Supervisor",
        lambda x: x["next"],
        edge_mapping
    )
    
    # 각 에이전트에서 다시 Supervisor로
    for agent_name in AGENTS.keys():
        workflow.add_edge(agent_name, "Supervisor")

# 시작점 설정
workflow.set_entry_point("Supervisor")

# 그래프 컴파일 (Human-in-the-Loop가 활성화된 경우 interrupt_before 설정)
if HUMAN_IN_THE_LOOP:
    memory = MemorySaver()
    graph = workflow.compile(checkpointer=memory, interrupt_before=["HumanReview"])
else:
    graph = workflow.compile()

def run_supervisor(query: str):
    """Supervisor를 실행하여 다중 에이전트 시스템을 동작시킵니다."""
    print(f"🔍 Query: {query}\n")
    config = {
        "configurable": {"thread_id": "main_thread"},
        "recursion_limit": DEFAULT_RECURSION_LIMIT
    }

    # 초기 질문으로 그래프 실행 시작
    if HUMAN_IN_THE_LOOP:
        # Human-in-the-Loop 모드에서는 interrupt를 처리
        state = {"messages": [HumanMessage(content=query)]}
        
        while True:
            # 다음 interrupt까지 실행
            result = graph.invoke(state, config)
            
            # interrupt가 발생했는지 확인
            graph_state = graph.get_state(config)
            if graph_state.next:
                # interrupt가 발생한 경우, HumanReview 노드가 실행될 예정
                # 현재 상태를 가져와서 human_review_node 실행
                current_state = graph_state.values
                human_result = human_review_node(current_state)
                
                # FINISH가 선택된 경우 종료
                if human_result["next"] == "FINISH":
                    print("\n🎯 Task completed by user choice!")
                    break
                
                # human_result를 기반으로 그래프 상태 업데이트
                update_data = {"next": human_result["next"]}
                if "messages" in human_result:
                    update_data["messages"] = human_result["messages"]
                
                # 그래프 상태를 업데이트
                graph.update_state(config, update_data)
                
                # 업데이트된 상태로 다음 iteration 준비
                state = None  # invoke(None, config)는 현재 상태에서 계속 실행
                
            else:
                # interrupt가 없는 경우 (작업 완료)
                print("\n🎯 Task completed successfully!")
                break
    else:
        # 기본 모드: 기존 방식
        events = graph.stream({"messages": [HumanMessage(content=query)]}, config)
        
        for chunk in events:
            for node, output in chunk.items():
                print(f"\n🤖 Node '{node}' output:")
                print("-" * 30)
                if "messages" in output:
                    for msg in output["messages"]:
                        print(f"Type: {type(msg).__name__}")
                        print(f"Content: {msg.content}")
                        print()
                else:
                    print(output)


if __name__ == "__main__":
    # OPENAI_API_KEY가 설정되어 있는지 확인
    if not os.getenv("OPENAI_API_KEY"):
        raise ValueError("OPENAI_API_KEY가 환경 변수에 설정되지 않았습니다.")

    # 그래프 다이어그램 출력
    from multiAgents.utils import print_graph
    print_graph(graph)

    # 테스트 실행
    print("\n" + "="*50)
    print("Multi-Agent SQL Generator")
    print("="*50)
    
    run_supervisor("users 테이블의 모든 정보를 가져오는 SQL을 만들어줘. 데이터베이스 이름은 'my_db'야.")
