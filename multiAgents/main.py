import os
from dotenv import load_dotenv
from langgraph.graph import StateGraph, END
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
from multiAgents.config import AGENTS, DEFAULT_RECURSION_LIMIT, DEBUG

# --- 그래프 생성 ---
workflow = StateGraph(AgentState)

# 노드 추가
workflow.add_node("Supervisor", supervisor_node)
workflow.add_node("SchemaAnalyzer", schema_node)
workflow.add_node("SQLGenerator", sql_node)

# 동적으로 엣지 정의 생성
edge_mapping = {agent: agent for agent in AGENTS.keys()}
edge_mapping["FINISH"] = END

# 엣지(연결) 정의
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

# 그래프 컴파일
graph = workflow.compile()

def run_supervisor(query: str):
    """Supervisor를 실행하여 다중 에이전트 시스템을 동작시킵니다."""
    print(f"🔍 Query: {query}\n")
    config = {
        "configurable": {"thread_id": "main_thread"},
        "recursion_limit": DEFAULT_RECURSION_LIMIT
    }

    # 초기 질문으로 그래프 실행 시작
    events = graph.stream({"messages": [HumanMessage(content=query)]}, config)
    
    for event in events:
        for node_name, node_output in event.items():
            if DEBUG and node_name != "Supervisor":  # Supervisor는 이미 자체 출력이 있음
                print(f"\n{'='*50}")
                print(f"WORKER: {node_name}")
                print(f"{'='*50}")
                
            if node_output and "messages" in node_output:
                for msg in node_output["messages"]:
                    msg_type = msg.__class__.__name__
                    
                    if msg_type == "AIMessage" and msg.tool_calls:
                        for tc in msg.tool_calls:
                            print(f"🔧 Calling tool: {tc['name']}")
                            if DEBUG:
                                print(f"   Args: {tc['args']}")
                    
                    elif msg_type == "ToolMessage":
                        print(f"📊 Tool result received")
                        if DEBUG:
                            print(f"   Result: {msg.content[:100]}...")
                    
                    elif msg_type == "AIMessage" and msg.content:
                        print(f"✅ {node_name} response:")
                        print(f"   {msg.content[:200]}...")
            
            if node_output and "next" in node_output and node_name == "Supervisor":
                if node_output["next"] == "FINISH":
                    print("\n🎯 Task completed successfully!")



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
