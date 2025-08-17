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
from multiAgents.config import DEFAULT_RECURSION_LIMIT

# --- 간단한 그래프 생성 ---
workflow = StateGraph(AgentState)

# Supervisor 노드만 추가 (에이전트 실행을 내부에서 관리)
workflow.add_node("Supervisor", supervisor_node)

# Supervisor에서 FINISH로만 연결
workflow.add_conditional_edges(
    "Supervisor",
    lambda x: "FINISH" if x.get("next") == "FINISH" else "Supervisor",
    {"FINISH": END, "Supervisor": "Supervisor"}
)

# 시작점 설정 - Supervisor에서 시작
workflow.set_entry_point("Supervisor")

# 그래프 컴파일
graph = workflow.compile()

def run_supervisor(query: str = None):
    """Supervisor를 실행하여 다중 에이전트 시스템을 동작시킵니다."""
    if query:
        print(f"🔍 Query: {query}\n")
    config = {
        "recursion_limit": DEFAULT_RECURSION_LIMIT
    }

    # 초기 메시지로 그래프 실행
    if query:
        initial_state = {"messages": [HumanMessage(content=query)]}
    else:
        initial_state = {"messages": []}
    
    # 그래프 스트림 실행
    events = graph.stream(initial_state, config)
    
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
    
    # UserCommunicator가 사용자 입력을 받으므로 query 없이 시작
    run_supervisor()
