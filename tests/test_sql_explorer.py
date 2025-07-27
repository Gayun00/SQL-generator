#!/usr/bin/env python3
"""
SQL Explorer Test - 탐색 쿼리 실행 기능 테스트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from workflow.nodes import sql_explorer, sql_analyzer
from workflow.state import SQLGeneratorState
from db.bigquery_client import bq_client
from rag.schema_embedder import schema_embedder
from rag.schema_retriever import schema_retriever

def initialize_test_environment():
    """테스트 환경 초기화"""
    print("🔍 테스트 환경 초기화 중...")
    
    # BigQuery 클라이언트와 스키마 초기화
    schema_info = schema_embedder.initialize_with_cache(bq_client)
    
    if not schema_info:
        print("❌ 스키마 정보 초기화 실패")
        return False
    
    print(f"✅ 스키마 정보 초기화 완료: {len(schema_info)}개 테이블")
    
    # 스키마 검색기 초기화
    if not schema_retriever.initialize():
        print("❌ 스키마 검색기 초기화 실패")
        return False
    
    print("✅ 스키마 검색기 초기화 완료")
    return True

async def test_column_values_exploration():
    """컬럼 값 탐색 테스트"""
    print("\n🔍 컬럼 값 탐색 테스트 중...")
    
    # 먼저 SQL_Analyzer로 불확실성 분석
    analyzer_state = {
        "userInput": "상태가 '활성'인 사용자 목록을 조회해줘",
        "isValid": True,
        "reason": None,
        "schemaInfo": None,
        "sqlQuery": None,
        "explanation": None,
        "finalOutput": None,
        "queryResults": None,
        "executionStatus": None,
        "uncertaintyAnalysis": None,
        "hasUncertainty": None,
        "explorationResults": None
    }
    
    # 불확실성 분석 실행
    analyzed_state = await sql_analyzer(analyzer_state)
    
    if not analyzed_state.get("hasUncertainty"):
        print("⚠️ 불확실성이 탐지되지 않아 탐색 테스트를 건너뜁니다.")
        return True
    
    # SQL_Explorer로 탐색 실행
    explorer_result = await sql_explorer(analyzed_state)
    
    exploration_results = explorer_result.get("explorationResults", {})
    executed_queries = exploration_results.get("executed_queries", 0)
    insights = exploration_results.get("insights", [])
    
    if executed_queries > 0:
        print("✅ 컬럼 값 탐색 성공!")
        print(f"   실행된 쿼리: {executed_queries}개")
        print(f"   생성된 인사이트: {len(insights)}개")
        
        for insight in insights:
            print(f"   💡 {insight}")
        
        return True
    else:
        print("❌ 탐색 쿼리가 실행되지 않았습니다.")
        return False

async def test_table_relationship_exploration():
    """테이블 관계 탐색 테스트"""
    print("\n🔍 테이블 관계 탐색 테스트 중...")
    
    # 먼저 SQL_Analyzer로 불확실성 분석
    analyzer_state = {
        "userInput": "사용자별 주문 내역과 주문한 상품 정보를 함께 조회해줘",
        "isValid": True,
        "reason": None,
        "schemaInfo": None,
        "sqlQuery": None,
        "explanation": None,
        "finalOutput": None,
        "queryResults": None,
        "executionStatus": None,
        "uncertaintyAnalysis": None,
        "hasUncertainty": None,
        "explorationResults": None
    }
    
    # 불확실성 분석 실행
    analyzed_state = await sql_analyzer(analyzer_state)
    
    if not analyzed_state.get("hasUncertainty"):
        print("⚠️ 불확실성이 탐지되지 않아 탐색 테스트를 건너뜁니다.")
        return True
    
    # SQL_Explorer로 탐색 실행
    explorer_result = await sql_explorer(analyzed_state)
    
    exploration_results = explorer_result.get("explorationResults", {})
    executed_queries = exploration_results.get("executed_queries", 0)
    results = exploration_results.get("results", [])
    
    if executed_queries > 0:
        print("✅ 테이블 관계 탐색 성공!")
        print(f"   실행된 쿼리: {executed_queries}개")
        print(f"   탐색 결과: {len(results)}개")
        
        # 테이블 관계 탐색 결과 확인
        table_relationship_found = any(
            result.get("uncertainty_type") == "table_relationship" 
            for result in results
        )
        
        if table_relationship_found:
            print("   📊 테이블 관계 정보 확인됨")
        
        return True
    else:
        print("❌ 탐색 쿼리가 실행되지 않았습니다.")
        return False

async def test_data_range_exploration():
    """데이터 범위 탐색 테스트"""
    print("\n🔍 데이터 범위 탐색 테스트 중...")
    
    # 먼저 SQL_Analyzer로 불확실성 분석
    analyzer_state = {
        "userInput": "최근 한달간 인기 상품 순위를 보여줘",
        "isValid": True,
        "reason": None,
        "schemaInfo": None,
        "sqlQuery": None,
        "explanation": None,
        "finalOutput": None,
        "queryResults": None,
        "executionStatus": None,
        "uncertaintyAnalysis": None,
        "hasUncertainty": None,
        "explorationResults": None
    }
    
    # 불확실성 분석 실행
    analyzed_state = await sql_analyzer(analyzer_state)
    
    if not analyzed_state.get("hasUncertainty"):
        print("⚠️ 불확실성이 탐지되지 않아 탐색 테스트를 건너뜁니다.")
        return True
    
    # SQL_Explorer로 탐색 실행
    explorer_result = await sql_explorer(analyzed_state)
    
    exploration_results = explorer_result.get("explorationResults", {})
    summary = exploration_results.get("summary", "")
    insights = exploration_results.get("insights", [])
    
    if "탐색 완료" in summary:
        print("✅ 데이터 범위 탐색 성공!")
        print(f"   탐색 요약: {summary}")
        print(f"   인사이트: {len(insights)}개")
        
        for insight in insights:
            print(f"   🎯 {insight}")
        
        return True
    else:
        print("❌ 데이터 범위 탐색 실패")
        print(f"   요약: {summary}")
        return False

async def test_no_uncertainty_handling():
    """불확실성이 없는 경우 처리 테스트"""
    print("\n🔍 불확실성 없는 경우 처리 테스트 중...")
    
    # 불확실성이 없는 상태로 SQL_Explorer 실행
    test_state = {
        "userInput": "간단한 테스트 쿼리",
        "isValid": True,
        "reason": None,
        "schemaInfo": None,
        "sqlQuery": None,
        "explanation": None,
        "finalOutput": None,
        "queryResults": None,
        "executionStatus": None,
        "uncertaintyAnalysis": {"has_uncertainty": False, "uncertainties": []},
        "hasUncertainty": False,
        "explorationResults": None
    }
    
    explorer_result = await sql_explorer(test_state)
    
    exploration_results = explorer_result.get("explorationResults", {})
    executed_queries = exploration_results.get("executed_queries", 0)
    summary = exploration_results.get("summary", "")
    
    if executed_queries == 0 and "탐색할 불확실성이 없습니다" in summary:
        print("✅ 불확실성 없는 경우 정상 처리!")
        print(f"   실행된 쿼리: {executed_queries}개 (예상대로 0개)")
        print(f"   요약: {summary}")
        return True
    else:
        print("❌ 불확실성 없는 경우 처리 실패")
        return False

async def test_error_handling():
    """오류 처리 테스트"""
    print("\n🔍 오류 처리 테스트 중...")
    
    # 잘못된 탐색 쿼리를 포함한 상태
    test_state = {
        "userInput": "오류 테스트",
        "isValid": True,
        "reason": None,
        "schemaInfo": None,
        "sqlQuery": None,
        "explanation": None,
        "finalOutput": None,
        "queryResults": None,
        "executionStatus": None,
        "uncertaintyAnalysis": {
            "has_uncertainty": True,
            "uncertainties": [{
                "type": "column_values",
                "description": "테스트 오류",
                "table": "nonexistent_table",
                "exploration_query": "SELECT * FROM nonexistent_table LIMIT 1"
            }]
        },
        "hasUncertainty": True,
        "explorationResults": None
    }
    
    explorer_result = await sql_explorer(test_state)
    
    exploration_results = explorer_result.get("explorationResults", {})
    results = exploration_results.get("results", [])
    
    if results and not results[0].get("success"):
        print("✅ 오류 처리 성공!")
        print(f"   오류 결과: {results[0].get('error', 'N/A')}")
        print("   오류가 적절히 캐치되고 처리됨")
        return True
    else:
        print("❌ 오류 처리 실패")
        return False

async def main():
    """전체 테스트 실행"""
    print("🚀 SQL Explorer 테스트 시작!")
    print("=" * 60)
    
    # 테스트 환경 초기화
    if not initialize_test_environment():
        print("❌ 테스트 환경 초기화 실패")
        return False
    
    tests = [
        ("컬럼 값 탐색", test_column_values_exploration),
        ("테이블 관계 탐색", test_table_relationship_exploration),
        ("데이터 범위 탐색", test_data_range_exploration),
        ("불확실성 없는 경우 처리", test_no_uncertainty_handling),
        ("오류 처리", test_error_handling)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n📋 테스트: {test_name}")
        print("-" * 40)
        
        try:
            result = await test_func()
            
            if result:
                passed += 1
                print(f"✅ {test_name} 통과")
            else:
                print(f"❌ {test_name} 실패")
                
        except Exception as e:
            print(f"💥 {test_name} 오류: {str(e)}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 60)
    print(f"🎯 테스트 결과: {passed}/{total} 통과")
    
    if passed == total:
        print("🎉 모든 테스트 통과!")
    else:
        print(f"⚠️ {total - passed}개 테스트 실패")
    
    return passed == total

if __name__ == "__main__":
    asyncio.run(main())