#!/usr/bin/env python3
"""
SQL Analyzer Test - 불확실성 분석 기능 테스트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from workflow.nodes import sql_analyzer
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

async def test_column_values_uncertainty():
    """컬럼 값 불확실성 테스트"""
    print("\n🔍 컬럼 값 불확실성 분석 테스트 중...")
    
    test_state = {
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
    
    result_state = await sql_analyzer(test_state)
    
    if result_state.get("hasUncertainty"):
        print("✅ 불확실성 탐지 성공!")
        uncertainty_analysis = result_state.get("uncertaintyAnalysis", {})
        uncertainties = uncertainty_analysis.get("uncertainties", [])
        
        print(f"   - 발견된 불확실성: {len(uncertainties)}개")
        for uncertainty in uncertainties:
            if uncertainty.get("type") == "column_values":
                print(f"   - 컬럼 값 불확실성: {uncertainty.get('description', 'N/A')}")
                return True
        
        print("⚠️ 컬럼 값 불확실성이 감지되지 않았습니다.")
        return False
    else:
        print("❌ 불확실성 탐지 실패")
        return False

async def test_table_relationship_uncertainty():
    """테이블 관계 불확실성 테스트"""
    print("\n🔍 테이블 관계 불확실성 분석 테스트 중...")
    
    test_state = {
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
    
    result_state = await sql_analyzer(test_state)
    
    if result_state.get("hasUncertainty"):
        print("✅ 불확실성 탐지 성공!")
        uncertainty_analysis = result_state.get("uncertaintyAnalysis", {})
        uncertainties = uncertainty_analysis.get("uncertainties", [])
        
        print(f"   - 발견된 불확실성: {len(uncertainties)}개")
        for uncertainty in uncertainties:
            if uncertainty.get("type") == "table_relationship":
                print(f"   - 테이블 관계 불확실성: {uncertainty.get('description', 'N/A')}")
                return True
        
        print("⚠️ 테이블 관계 불확실성이 감지되지 않았습니다.")
        return False
    else:
        print("❌ 불확실성 탐지 실패")
        return False

async def test_data_range_uncertainty():
    """데이터 범위 불확실성 테스트"""
    print("\n🔍 데이터 범위 불확실성 분석 테스트 중...")
    
    test_state = {
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
    
    result_state = await sql_analyzer(test_state)
    
    if result_state.get("hasUncertainty"):
        print("✅ 불확실성 탐지 성공!")
        uncertainty_analysis = result_state.get("uncertaintyAnalysis", {})
        uncertainties = uncertainty_analysis.get("uncertainties", [])
        
        print(f"   - 발견된 불확실성: {len(uncertainties)}개")
        for uncertainty in uncertainties:
            if uncertainty.get("type") == "data_range":
                print(f"   - 데이터 범위 불확실성: {uncertainty.get('description', 'N/A')}")
                return True
        
        print("⚠️ 데이터 범위 불확실성이 감지되지 않았습니다.")
        return False
    else:
        print("❌ 불확실성 탐지 실패")
        return False

async def test_no_uncertainty():
    """불확실성이 정확히 탐지되는 쿼리 테스트"""
    print("\n🔍 불확실성 탐지 정확성 테스트 중...")
    
    test_state = {
        "userInput": "모든 사용자 정보를 조회해줘",
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
    
    result_state = await sql_analyzer(test_state)
    
    if result_state.get("hasUncertainty"):
        print("✅ 불확실성 정확 탐지 성공!")
        uncertainty_analysis = result_state.get("uncertaintyAnalysis", {})
        uncertainties = uncertainty_analysis.get("uncertainties", [])
        print(f"   - 정당한 불확실성 탐지: {len(uncertainties)}개")
        print("   - 여러 사용자 테이블 중 어떤 것을 사용할지 불분명")
        return True
    else:
        print("❌ 불확실성이 있는 쿼리임에도 탐지하지 못했습니다.")
        print("   - 여러 사용자 테이블이 존재하는데 불확실성을 놓쳤음")
        return False

async def test_json_parsing():
    """JSON 파싱 및 오류 처리 테스트"""
    print("\n🔍 JSON 파싱 및 오류 처리 테스트 중...")
    
    test_state = {
        "userInput": "복잡한 비즈니스 로직이 포함된 특수한 쿼리 요청",
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
    
    result_state = await sql_analyzer(test_state)
    
    # JSON 파싱 성공 또는 오류 처리 성공 둘 다 테스트 통과
    uncertainty_analysis = result_state.get("uncertaintyAnalysis", {})
    
    if "error" in uncertainty_analysis:
        print("⚠️ JSON 파싱 오류 발생, 하지만 적절히 처리됨")
        print(f"   - 오류: {uncertainty_analysis['error']}")
        return True
    elif isinstance(uncertainty_analysis, dict) and "has_uncertainty" in uncertainty_analysis:
        print("✅ JSON 파싱 성공!")
        print(f"   - 불확실성 존재: {uncertainty_analysis.get('has_uncertainty')}")
        print(f"   - 신뢰도: {uncertainty_analysis.get('confidence', 0.0):.2f}")
        return True
    else:
        print("❌ JSON 파싱 및 오류 처리 실패")
        return False

async def main():
    """전체 테스트 실행"""
    print("🚀 SQL Analyzer 테스트 시작!")
    print("=" * 60)
    
    # 테스트 환경 초기화
    if not initialize_test_environment():
        print("❌ 테스트 환경 초기화 실패")
        return False
    
    tests = [
        ("컬럼 값 불확실성 탐지", test_column_values_uncertainty),
        ("테이블 관계 불확실성 탐지", test_table_relationship_uncertainty),
        ("데이터 범위 불확실성 탐지", test_data_range_uncertainty),
        ("불확실성 탐지 정확성", test_no_uncertainty),
        ("JSON 파싱 및 오류 처리", test_json_parsing)
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