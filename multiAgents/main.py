import os
from dotenv import load_dotenv
from langgraph.graph import StateGraph, END

# .env 파일에서 환경 변수 로드
load_dotenv()

# LangSmith 추적을 위한 환경 변수 설정 (선택 사항)
# os.environ["LANGCHAIN_TRACING_V2"] = "true"
# os.environ["LANGCHAIN_API_KEY"] = "YOUR_LANGCHAIN_API_KEY"

from multiAgents.state import AgentState
from multiAgents.supervisor import supervisor_node
from multiAgents.agents.schema_analyzer_agent import schema_node
from multiAgents.agents.sql_generator_agent import sql_node

# --- 그래프 생성 및 실행 ---
# 에이전트들을 연결하여 워크플로우 그래프를 생성합니다.

workflow = StateGraph(AgentState)

# 노드 추가
workflow.add_node("Supervisor", supervisor_node)
workflow.add_node("SchemaAnalyzer", schema_node)
workflow.add_node("SQLGenerator", sql_node)

# 엣지(연결) 정의
workflow.add_conditional_edges(
    "Supervisor",
    lambda x: x["next"],
    {
        "SchemaAnalyzer": "SchemaAnalyzer",
        "SQLGenerator": "SQLGenerator",
        "FINISH": END
    }
)
workflow.add_edge("SchemaAnalyzer", "Supervisor")
workflow.add_edge("SQLGenerator", "Supervisor")

# 시작점 설정
workflow.set_entry_point("Supervisor")

# 그래프 컴파일
graph = workflow.compile()

def run_supervisor(query: str):
    """Supervisor를 실행하여 다중 에이전트 시스템을 동작시킵니다."""
    print(f"입력: {query}\n")
    for s in graph.stream({"messages": [("user", query)]}):
        if "__end__" not in s:
            print(s)
            print("----\n")

if __name__ == "__main__":
    # OPENAI_API_KEY가 설정되어 있는지 확인
    if not os.getenv("OPENAI_API_KEY"):
        raise ValueError("OPENAI_API_KEY가 환경 변수에 설정되지 않았습니다.")

    # 그래프 다이어그램 출력
    from multiAgents.utils import print_graph
    print_graph(graph)

    run_supervisor("users 테이블의 모든 정보를 가져오는 SQL을 만들어줘. 데이터베이스 이름은 'my_db'야.")
