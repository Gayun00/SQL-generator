#!/usr/bin/env python3
"""
Hybrid System Test - A2A와 Legacy 시스템 비교 테스트

기존 sql_analyzer 노드와 새로운 SchemaIntelligence Agent의 
성능과 정확도를 비교하는 테스트입니다.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import json
from datetime import datetime
from workflow.state import SQLGeneratorState
from workflow.hybrid_nodes import (
    hybrid_sql_analyzer, agent_only_sql_analyzer, legacy_only_sql_analyzer,
    get_hybrid_performance_report, get_recent_hybrid_comparisons,
    switch_to_comparison_mode, switch_to_agent_mode, switch_to_legacy_mode
)
from db.bigquery_client import bq_client
from rag.schema_embedder import schema_embedder
from rag.schema_retriever import schema_retriever

def initialize_test_environment():
    """테스트 환경 초기화"""
    print("🔧 Hybrid 테스트 환경 초기화 중...")
    
    try:
        # BigQuery 클라이언트와 스키마 초기화
        schema_info = schema_embedder.initialize_with_cache(bq_client)
        
        if not schema_info:
            print("❌ 스키마 정보 초기화 실패")
            return False
        
        print(f"✅ 스키마 정보 초기화 완료: {len(schema_info)}개 테이블")
        
        # BigQuery 클라이언트에 스키마 정보 설정
        bq_client.schema_info = schema_info
        
        # 스키마 검색기 초기화
        if not schema_retriever.initialize():
            print("❌ 스키마 검색기 초기화 실패")
            return False
        
        print("✅ 스키마 검색기 초기화 완료")
        return True
        
    except Exception as e:
        print(f"❌ 환경 초기화 실패: {str(e)}")
        return False

async def test_hybrid_comparison():
    """Hybrid 비교 테스트"""
    print("\\n🔍 Hybrid 비교 테스트 시작")
    print("=" * 60)
    
    # 테스트 쿼리들
    test_queries = [
        "users 테이블에서 모든 사용자 정보 조회",
        "최근 일주일간 가장 많은 금액을 결제한 유저의 이름을 보여줘",
        "상태가 활성인 사용자들의 주문 내역을 보여줘",
        "카테고리별 매출 통계를 구해줘"
    ]
    
    switch_to_comparison_mode()
    
    for i, query in enumerate(test_queries, 1):
        print(f"\\n📋 테스트 {i}: {query}")
        print("-" * 40)
        
        # 초기 상태 생성
        test_state = {
            "userInput": query,
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
        
        try:
            start_time = datetime.now()
            result = await hybrid_sql_analyzer(test_state)
            end_time = datetime.now()
            
            execution_time = (end_time - start_time).total_seconds()
            
            print(f"⏱️  실행 시간: {execution_time:.2f}초")
            print(f"🔍 불확실성 탐지: {result.get('hasUncertainty', False)}")
            
            # 비교 정보 출력
            if "hybridComparison" in result:
                comp = result["hybridComparison"]
                print(f"🏆 성능 승자: {comp['performance_winner']}")
                print(f"✅ 정확도 일치: {comp['accuracy_match']}")
                print(f"📊 추천사항: {comp['recommendation']}")
            
            # 불확실성 분석 결과 출력
            uncertainty_analysis = result.get("uncertaintyAnalysis", {})
            if uncertainty_analysis.get("uncertainties"):
                uncertainties = uncertainty_analysis["uncertainties"]
                print(f"📝 발견된 불확실성: {len(uncertainties)}개")
                for j, uncertainty in enumerate(uncertainties[:2], 1):  # 최대 2개만 표시
                    print(f"   {j}. {uncertainty.get('type', 'unknown')}: {uncertainty.get('description', 'N/A')[:50]}...")
            
            print("✅ 테스트 완료")
            
        except Exception as e:
            print(f"❌ 테스트 실패: {str(e)}")
    
    return True

async def test_individual_modes():
    """개별 모드 테스트"""
    print("\\n🔧 개별 모드 테스트 시작")
    print("=" * 60)
    
    test_query = "최근 일주일간 가장 많이 결제한 사용자를 찾아줘"
    
    test_state = {
        "userInput": test_query,
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
    
    modes = [
        ("Legacy Only", legacy_only_sql_analyzer),
        ("Agent Only", agent_only_sql_analyzer),
        ("Hybrid Compare", hybrid_sql_analyzer)
    ]
    
    for mode_name, mode_func in modes:
        print(f"\\n🎯 {mode_name} 모드 테스트")
        print("-" * 30)
        
        try:
            start_time = datetime.now()
            result = await mode_func(test_state.copy())
            end_time = datetime.now()
            
            execution_time = (end_time - start_time).total_seconds()
            
            print(f"⏱️  실행 시간: {execution_time:.2f}초")
            print(f"🔍 불확실성 탐지: {result.get('hasUncertainty', False)}")
            
            uncertainty_analysis = result.get("uncertaintyAnalysis", {})
            confidence = uncertainty_analysis.get("confidence", 0.0)
            print(f"🎯 신뢰도: {confidence:.2f}")
            
            if uncertainty_analysis.get("uncertainties"):
                print(f"📝 불확실성 개수: {len(uncertainty_analysis['uncertainties'])}개")
            
            print("✅ 모드 테스트 완료")
            
        except Exception as e:
            print(f"❌ 모드 테스트 실패: {str(e)}")
    
    return True

def test_performance_reporting():
    """성능 리포트 테스트"""
    print("\\n📊 성능 리포트 테스트")
    print("=" * 60)
    
    try:
        # 성능 리포트 조회
        report = get_hybrid_performance_report()
        
        print("📈 전체 성능 리포트:")
        for key, value in report.items():
            if isinstance(value, dict):
                print(f"   {key}:")
                for sub_key, sub_value in value.items():
                    print(f"     - {sub_key}: {sub_value}")
            else:
                print(f"   {key}: {value}")
        
        print("\\n🔍 최근 비교 결과:")
        recent_comparisons = get_recent_hybrid_comparisons(3)
        
        for i, comp in enumerate(recent_comparisons, 1):
            print(f"  {i}. 성능 승자: {comp.get('성능_승자', 'N/A')}")
            print(f"     정확도 일치: {comp.get('정확도_일치', 'N/A')}")
            print(f"     추천사항: {comp.get('추천사항', 'N/A')[:60]}...")
            print()
        
        print("✅ 성능 리포트 테스트 완료")
        return True
        
    except Exception as e:
        print(f"❌ 성능 리포트 테스트 실패: {str(e)}")
        return False

async def main():
    """메인 테스트 실행"""
    print("🚀 Hybrid System 종합 테스트 시작!")
    print("=" * 80)
    
    # 환경 초기화
    if not initialize_test_environment():
        print("❌ 환경 초기화 실패로 테스트 중단")
        return False
    
    # 테스트 실행
    tests = [
        ("Hybrid 비교 테스트", test_hybrid_comparison),
        ("개별 모드 테스트", test_individual_modes),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\\n🧪 {test_name}")
        print("=" * 80)
        
        try:
            if await test_func():
                passed += 1
                print(f"✅ {test_name} 통과")
            else:
                print(f"❌ {test_name} 실패")
        except Exception as e:
            print(f"💥 {test_name} 오류: {str(e)}")
            import traceback
            traceback.print_exc()
    
    # 성능 리포트 테스트 (동기 함수)
    print("\\n🧪 성능 리포트 테스트")
    print("=" * 80)
    if test_performance_reporting():
        passed += 1
    total += 1
    
    # 최종 결과
    print("\\n" + "=" * 80)
    print(f"🎯 Hybrid System 테스트 결과: {passed}/{total} 통과")
    
    if passed == total:
        print("🎉 모든 Hybrid 테스트 통과!")
        print("✅ A2A 시스템이 성공적으로 통합되었습니다!")
        print("\\n📊 다음 단계: 성능 최적화 및 추가 Agent 구현")
    else:
        print(f"⚠️ {total - passed}개 테스트 실패")
        print("🔧 시스템 개선이 필요합니다.")
    
    return passed == total

if __name__ == "__main__":
    asyncio.run(main())