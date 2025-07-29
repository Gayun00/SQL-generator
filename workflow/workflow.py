# =================================================================
# DEPRECATED: 기존 Langgraph 워크플로우 (A2A 전환으로 비활성화됨)
# =================================================================
# 
# 이 파일은 A2A (Agent-to-Agent) 아키텍처 전환으로 더 이상 사용되지 않습니다.
# 대신 workflow/a2a_workflow.py를 사용하세요.
#
# 변경 이유:
# - 고정된 플로우 → 동적 플로우
# - Langgraph 의존성 제거 → 순수 A2A 아키텍처 
# - Agent 결과 기반 플로우 조정 지원
#
# =================================================================

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def create_workflow():
    """
    DEPRECATED: A2A 워크플로우로 전환됨
    
    호환성을 위해 유지되지만, 새로운 A2A 워크플로우 사용을 권장합니다.
    """
    import warnings
    warnings.warn(
        "create_workflow()는 더 이상 사용되지 않습니다. "
        "workflow.a2a_workflow.create_a2a_workflow()를 사용하세요.",
        DeprecationWarning,
        stacklevel=2
    )
    
    # 기존 코드와의 호환성을 위해 에러가 아닌 경고만 표시
    print("⚠️ 경고: Langgraph 워크플로우는 비활성화되었습니다.")
    print("💡 A2A 워크플로우 사용을 권장합니다: python workflow/a2a_workflow.py")
    
    raise NotImplementedError("A2A 워크플로우로 전환되었습니다. workflow/a2a_workflow.py를 사용하세요.")

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
                "finalOutput": None,
                "queryResults": None,
                "executionStatus": None,
                "uncertaintyAnalysis": None,
                "hasUncertainty": None,
                "explorationResults": None,
                "needsClarification": None,
                "clarificationQuestions": None,
                "clarificationSummary": None,
                "userAnswers": None
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