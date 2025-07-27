#!/usr/bin/env python3
"""
SQL Generator - Main Entry Point
BigQuery 스키마 기반 SQL 쿼리 자동 생성 시스템
"""

import asyncio
from workflow.workflow import create_workflow
from db.bigquery_client import bq_client
from rag.schema_embedder import schema_embedder
from rag.schema_retriever import schema_retriever

async def initialize_system():
    """시스템 초기화 - 캐시 기반 스키마 로딩"""
    print("🚀 SQL Generator 시스템 초기화 중...")
    print("=" * 60)
    
    # 캐시 기반 스키마 초기화 (BigQuery API 호출 최소화)
    schema_info = schema_embedder.initialize_with_cache(bq_client)
    
    if not schema_info:
        print("❌ 스키마 정보 초기화에 실패했습니다. 시스템을 종료합니다.")
        return False
    
    # BigQuery 클라이언트에 스키마 정보 설정 (노드에서 사용할 수 있도록)
    bq_client.schema_info = schema_info
    
    # 스키마 검색기 초기화
    print("\n🔍 스키마 검색기 초기화 중...")
    if not schema_retriever.initialize():
        print("❌ 스키마 검색기 초기화에 실패했습니다. 시스템을 종료합니다.")
        return False
    
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
    
    return True

async def main():
    """메인 함수"""
    try:
        # 시스템 초기화
        if not await initialize_system():
            return
        
        # 워크플로우 생성 및 실행
        app = create_workflow()
        
        print("\n🚀 SQL Generator A2A 워크플로우 시작!")
        print("💡 사용 가능한 명령:")
        print("   - SQL 생성 요청을 자연어로 입력하세요")
        print("   - 'quit', 'exit', '종료'로 프로그램 종료")
        print("=" * 60)
        
        while True:
            # 사용자 입력 받기
            user_input = input("\n💬 SQL 생성 요청을 입력하세요: ").strip()
            
            if user_input.lower() in ['quit', 'exit', '종료']:
                print("👋 SQL Generator를 종료합니다.")
                break
            
            if not user_input:
                print("⚠️ 입력이 비어있습니다. 다시 입력해주세요.")
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
            
            print(f"\n📝 처리 중: {user_input}")
            print("-" * 40)
            
            try:
                # 워크플로우 실행
                result = await app.ainvoke(initial_state)
                
                print("\n" + "=" * 60)
                print("🎯 처리 결과:")
                
                if result.get('finalOutput'):
                    print(result['finalOutput'])
                else:
                    print(f"✅ 유효성: {result.get('isValid')}")
                    if result.get('reason'):
                        print(f"💡 이유: {result.get('reason')}")
                    if result.get('sqlQuery'):
                        print(f"📋 생성된 SQL: {result.get('sqlQuery')}")
                    if result.get('explanation'):
                        print(f"📖 설명: {result.get('explanation')}")
                
            except Exception as e:
                print(f"\n❌ 처리 중 오류가 발생했습니다: {str(e)}")
                print("다시 시도해주세요.")
                
    except KeyboardInterrupt:
        print("\n👋 사용자가 프로그램을 중단했습니다.")
    except Exception as e:
        print(f"\n💥 예상치 못한 오류가 발생했습니다: {str(e)}")
        print("프로그램을 종료합니다.")

if __name__ == "__main__":
    asyncio.run(main())