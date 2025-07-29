#!/usr/bin/env python3
"""
워크플로우 테스트 스크립트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
from workflow.workflow import create_workflow
from db.bigquery_client import bq_client
from rag.schema_embedder import schema_embedder
from rag.schema_retriever import schema_retriever

async def test_specific_query():
    """특정 쿼리로 워크플로우 테스트"""
    
    # 시스템 초기화
    print("🚀 SQL Generator 시스템 초기화 중...")
    print("=" * 60)
    
    schema_info = schema_embedder.initialize_with_cache(bq_client)
    if not schema_info:
        print("❌ 스키마 정보 초기화에 실패했습니다.")
        return
    
    bq_client.schema_info = schema_info
    
    if not schema_retriever.initialize():
        print("❌ 스키마 검색기 초기화에 실패했습니다.")
        return
    
    print(f"✅ 시스템 초기화 완료! ({len(schema_info)}개 테이블)")
    
    # 워크플로우 생성
    app = create_workflow()
    
    # 테스트 쿼리
    test_query = "최근 일주일 간 가장 많은 금액을 결제한 유저의 이름을 보여줘"
    
    initial_state = {
        "userInput": test_query,
        "isValid": False,
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
    
    print(f"\n📝 테스트 쿼리: {test_query}")
    print("=" * 60)
    
    try:
        # 워크플로우 실행
        result = await app.ainvoke(initial_state)
        
        print("\n" + "=" * 60)
        print("🎯 최종 결과:")
        print(f"✅ 유효성: {result.get('isValid')}")
        
        if result.get('reason'):
            print(f"💡 이유: {result.get('reason')}")
        
        if result.get('uncertaintyAnalysis'):
            print(f"🔍 불확실성 분석: {result.get('hasUncertainty')}")
        
        if result.get('explorationResults'):
            print(f"🧠 추가 쿼리 실행: 완료")
        
        if result.get('sqlQuery'):
            print(f"📋 생성된 SQL:\n{result.get('sqlQuery')}")
        
        if result.get('queryResults'):
            print(f"📊 실행 결과: {result.get('executionStatus')}")
        
        if result.get('explanation'):
            print(f"📖 설명: {result.get('explanation')}")
        
        if result.get('finalOutput'):
            print(f"📄 최종 출력:\n{result.get('finalOutput')}")
            
        return result
        
    except Exception as e:
        print(f"❌ 오류가 발생했습니다: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    asyncio.run(test_specific_query())