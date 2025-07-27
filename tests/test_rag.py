#!/usr/bin/env python3
"""
RAG 시스템 테스트 스크립트
"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.bigquery_client import bq_client
from rag.schema_embedder import schema_embedder
from rag.schema_retriever import schema_retriever

async def test_rag_system():
    """RAG 시스템 테스트"""
    print("🧪 RAG 시스템 테스트 시작")
    print("=" * 60)
    
    # 1. BigQuery 연결 및 스키마 수집
    print("🔗 BigQuery 연결 중...")
    if not bq_client.connect():
        print("❌ BigQuery 연결 실패")
        return False
    
    print("🔍 스키마 정보 수집 중...")
    schema_info = bq_client.initialize_schema()
    if not schema_info:
        print("❌ 스키마 정보 수집 실패")
        return False
    
    # 2. RAG 시스템 초기화
    print("\n🧠 RAG 시스템 초기화...")
    
    print("   - 벡터스토어 초기화...")
    if not schema_embedder.initialize_vectorstore():
        print("❌ 벡터스토어 초기화 실패")
        return False
    
    print("   - 스키마 임베딩...")
    if not schema_embedder.embed_schemas(schema_info):
        print("❌ 스키마 임베딩 실패")
        return False
    
    print("   - 검색기 초기화...")
    if not schema_retriever.initialize():
        print("❌ 검색기 초기화 실패")
        return False
    
    # 3. 검색 테스트
    print("\n🔍 검색 기능 테스트")
    print("-" * 40)
    
    test_queries = [
        "사용자 정보를 조회하고 싶어",
        "주문 데이터에서 매출을 계산해줘",
        "제품별 판매량을 보여줘",
        "날짜별 접속자 수 통계",
        "고객의 구매 이력을 분석하고 싶어"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{i}. 테스트 쿼리: '{query}'")
        print("-" * 30)
        
        # 관련 테이블 검색
        relevant_tables = schema_retriever.get_relevant_tables(query, top_k=3)
        
        if relevant_tables:
            print(f"📊 발견된 관련 테이블 수: {len(relevant_tables)}")
            for j, table in enumerate(relevant_tables[:2], 1):  # 상위 2개만 표시
                print(f"   {j}. {table['table_name']}")
                if table['description']:
                    print(f"      설명: {table['description']}")
                if table['matched_elements']:
                    print(f"      매칭 요소: {', '.join(table['matched_elements'][:3])}")
        else:
            print("   ❌ 관련 테이블을 찾을 수 없습니다.")
        
        # 컨텍스트 요약 생성
        context = schema_retriever.create_context_summary(query, max_tables=2)
        print(f"   📝 컨텍스트 길이: {len(context)} 문자")
    
    # 4. 통계 정보
    print(f"\n📈 RAG 시스템 통계")
    print("-" * 40)
    stats = schema_retriever.get_statistics()
    print(f"상태: {stats.get('status', 'unknown')}")
    print(f"문서 수: {stats.get('document_count', 0)}")
    print(f"컬렉션: {stats.get('collection_name', 'unknown')}")
    
    print(f"\n🎉 RAG 시스템 테스트 완료!")
    print("이제 'make run'으로 전체 시스템을 실행할 수 있습니다.")
    
    return True

def main():
    """메인 함수"""
    try:
        result = asyncio.run(test_rag_system())
        if not result:
            print("\n❌ RAG 시스템 테스트에 실패했습니다.")
            print("오류를 확인하고 다시 시도해주세요.")
            return 1
        return 0
            
    except KeyboardInterrupt:
        print("\n👋 사용자가 테스트를 중단했습니다.")
        return 1
    except Exception as e:
        print(f"\n💥 예상치 못한 오류가 발생했습니다: {str(e)}")
        print("스택 트레이스:")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())