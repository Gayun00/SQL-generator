import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from langgraph.graph import StateGraph, END
from workflow.state import SQLGeneratorState
from workflow.nodes import clarifier, wait_for_user, sql_generator, explainer, orchestrator, final_answer

def create_workflow():
    """LangGraph 워크플로우 생성 및 구성"""
    
    workflow = StateGraph(SQLGeneratorState)
    
    # 노드 추가
    workflow.add_node("clarifier", clarifier)
    workflow.add_node("wait_for_user", wait_for_user)
    workflow.add_node("sql_generator", sql_generator)
    workflow.add_node("explainer", explainer)
    workflow.add_node("final_answer", final_answer)
    
    # 시작점 설정
    workflow.set_entry_point("clarifier")
    
    # 조건부 엣지 추가 (Orchestrator 로직)
    workflow.add_conditional_edges(
        "clarifier",
        orchestrator,
        {
            "wait_for_user": "wait_for_user",
            "clarifier": "clarifier",
            "sql_generator": "sql_generator", 
            "explainer": "explainer",
            "final_answer": "final_answer"
        }
    )
    
    workflow.add_conditional_edges(
        "wait_for_user",
        orchestrator,
        {
            "wait_for_user": "wait_for_user",
            "clarifier": "clarifier",
            "sql_generator": "sql_generator",
            "explainer": "explainer", 
            "final_answer": "final_answer"
        }
    )
    
    workflow.add_conditional_edges(
        "sql_generator",
        orchestrator,
        {
            "wait_for_user": "wait_for_user",
            "clarifier": "clarifier",
            "sql_generator": "sql_generator",
            "explainer": "explainer",
            "final_answer": "final_answer"
        }
    )
    
    workflow.add_conditional_edges(
        "explainer", 
        orchestrator,
        {
            "wait_for_user": "wait_for_user",
            "clarifier": "clarifier",
            "sql_generator": "sql_generator",
            "explainer": "explainer",
            "final_answer": "final_answer"
        }
    )
    
    # 종료점 설정
    workflow.add_edge("final_answer", END)
    
    return workflow.compile()

if __name__ == "__main__":
    import asyncio
    from db.bigquery_client import bq_client
    
    async def main():
        # BigQuery 초기화
        print("🔗 BigQuery 연결 및 스키마 초기화 중...")
        if not bq_client.connect():
            print("❌ BigQuery 연결 실패. 프로그램을 종료합니다.")
            return
        
        if not bq_client.initialize_schema():
            print("❌ 스키마 초기화 실패. 프로그램을 종료합니다.")
            return
        
        # 워크플로우 생성
        app = create_workflow()
        
        print("🚀 SQL Generator A2A 워크플로우 시작!")
        print("=" * 60)
        
        while True:
            # 사용자 입력 받기
            user_input = input("\n💬 SQL 생성 요청을 입력하세요 (종료하려면 'quit' 또는 'exit' 입력): ")
            
            if user_input.lower() in ['quit', 'exit', '종료']:
                print("👋 워크플로우를 종료합니다.")
                break
            
            if not user_input.strip():
                print("⚠️  입력이 비어있습니다. 다시 입력해주세요.")
                continue
            
            # 초기 상태 설정
            initial_state = {
                "userInput": user_input,
                "isValid": False,  # clarifier에서 검증하도록 초기값은 False
                "reason": None,
                "schemaInfo": None,
                "sqlQuery": None,
                "explanation": None,
                "finalOutput": None
            }
            
            print(f"📝 사용자 입력: {user_input}")
            print("=" * 60)
            
            try:
                # 워크플로우 실행
                result = await app.ainvoke(initial_state)
                
                print("=" * 60)
                print("🎯 최종 결과:")
                print(f"✅ 유효성: {result.get('isValid')}")
                if result.get('reason'):
                    print(f"💡 이유: {result.get('reason')}")
                if result.get('sqlQuery'):
                    print(f"📋 생성된 SQL: {result.get('sqlQuery')}")
                if result.get('explanation'):
                    print(f"📖 설명: {result.get('explanation')}")
                if result.get('finalOutput'):
                    print(f"📄 최종 출력: {result.get('finalOutput')}")
                
            except Exception as e:
                print(f"❌ 오류가 발생했습니다: {str(e)}")
                print("다시 시도해주세요.")
    
    asyncio.run(main())