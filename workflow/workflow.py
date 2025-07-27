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
    from rag.schema_embedder import schema_embedder
    from rag.schema_retriever import schema_retriever
    
    async def main():
        # 캐시 기반 시스템 초기화
        print("🚀 SQL Generator 시스템 초기화 중...")
        print("=" * 60)
        
        # 캐시 기반 스키마 초기화 (BigQuery API 호출 최소화)
        schema_info = schema_embedder.initialize_with_cache(bq_client)
        
        if not schema_info:
            print("❌ 스키마 정보 초기화에 실패했습니다. 시스템을 종료합니다.")
            return
        
        # BigQuery 클라이언트에 스키마 정보 설정 (노드에서 사용할 수 있도록)
        bq_client.schema_info = schema_info
        
        # 스키마 검색기 초기화
        print("\n🔍 스키마 검색기 초기화 중...")
        if not schema_retriever.initialize():
            print("❌ 스키마 검색기 초기화에 실패했습니다. 시스템을 종료합니다.")
            return
        
        # 초기화 완료 정보 출력
        print(f"\n✅ 시스템 초기화 완료!")
        print(f"📊 BigQuery: {len(schema_info)}개 테이블의 스키마 정보 로드")
        
        # RAG 통계 정보
        rag_stats = schema_retriever.get_statistics()
        if rag_stats.get("status") == "ready":
            print(f"🧠 RAG: {rag_stats.get('document_count', 0)}개 문서가 임베딩됨")
            if rag_stats.get('cache_last_updated'):
                print(f"📅 캐시: {rag_stats.get('cache_last_updated', '').split('T')[0]} 업데이트")
        
        print("=" * 60)
        
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