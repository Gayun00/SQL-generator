#!/usr/bin/env python3
"""
Hybrid Workflow 실제 동작 테스트

기존 workflow에 Hybrid 시스템을 통합해서 실제로 작동하는지 테스트합니다.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
from workflow.state import SQLGeneratorState
from workflow.hybrid_nodes import (
    hybrid_sql_analyzer, 
    get_hybrid_performance_report,
    switch_to_comparison_mode
)
from db.bigquery_client import bq_client
from rag.schema_embedder import schema_embedder
from rag.schema_retriever import schema_retriever

async def test_hybrid_workflow():
    """Hybrid Workflow 실제 테스트"""
    print("🚀 Hybrid Workflow 실제 동작 테스트 시작!")
    print("=" * 60)
    
    # 환경 초기화
    print("🔧 환경 초기화 중...")
    try:
        schema_info = schema_embedder.initialize_with_cache(bq_client)
        if not schema_info:
            print("❌ 스키마 초기화 실패")
            return False
        
        bq_client.schema_info = schema_info
        
        if not schema_retriever.initialize():
            print("❌ 스키마 검색기 초기화 실패")
            return False
        
        print(f"✅ 환경 초기화 완료 ({len(schema_info)}개 테이블)")
        
    except Exception as e:
        print(f"❌ 환경 초기화 실패: {str(e)}")
        return False
    
    # Hybrid 모드 설정
    switch_to_comparison_mode()
    print("✅ Hybrid 비교 모드 활성화")
    
    # 테스트 시나리오
    test_queries = [
        {
            "name": "단순 조회",
            "query": "users 테이블의 모든 데이터를 보여줘",
            "expected": "불확실성 낮음"
        },
        {
            "name": "복잡한 분석", 
            "query": "최근 일주일간 가장 많은 금액을 결제한 유저의 이름을 보여줘",
            "expected": "불확실성 높음"
        },
        {
            "name": "조건부 조회",
            "query": "상태가 활성인 사용자들의 주문 내역을 보여줘", 
            "expected": "컬럼값 불확실성"
        }
    ]
    
    print(f"\n📋 {len(test_queries)}개 시나리오 테스트 시작")
    print("=" * 60)
    
    success_count = 0
    
    for i, test_case in enumerate(test_queries, 1):
        print(f"\n🧪 테스트 {i}: {test_case['name']}")
        print(f"쿼리: {test_case['query']}")
        print("-" * 40)
        
        # 초기 상태 생성
        initial_state = {
            "userInput": test_case["query"],
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
            "explorationResults": None,
            "needsClarification": None,
            "clarificationQuestions": None,
            "clarificationSummary": None,
            "userAnswers": None
        }
        
        try:
            # Hybrid SQL Analyzer 실행
            print("🔄 Hybrid SQL Analyzer 실행 중...")
            result_state = await hybrid_sql_analyzer(initial_state)
            
            # 결과 분석
            print(f"✅ 실행 완료!")
            print(f"   불확실성 탐지: {result_state.get('hasUncertainty', False)}")
            
            uncertainty_analysis = result_state.get('uncertaintyAnalysis', {})
            confidence = uncertainty_analysis.get('confidence', 0.0)
            print(f"   신뢰도: {confidence:.2f}")
            
            uncertainties = uncertainty_analysis.get('uncertainties', [])
            if uncertainties:
                print(f"   발견된 불확실성: {len(uncertainties)}개")
                for j, uncertainty in enumerate(uncertainties[:2], 1):
                    uncertainty_type = uncertainty.get('type', 'unknown')
                    description = uncertainty.get('description', 'N/A')[:50]
                    print(f"     {j}. {uncertainty_type}: {description}...")
            
            # Hybrid 비교 정보
            if "hybridComparison" in result_state:
                comp = result_state["hybridComparison"]
                print(f"   🏆 성능 우승: {comp['performance_winner']}")
                print(f"   📊 정확도 일치: {comp['accuracy_match']}")
                print(f"   ⏱️  시간 차이: Legacy {comp['legacy_time']:.2f}s vs Agent {comp['agent_time']:.2f}s")
            
            success_count += 1
            print("✅ 테스트 성공")
            
        except Exception as e:
            print(f"❌ 테스트 실패: {str(e)}")
            import traceback
            traceback.print_exc()
    
    # 전체 성능 리포트
    print(f"\n📊 전체 성능 리포트")
    print("=" * 60)
    
    try:
        report = get_hybrid_performance_report()
        
        for key, value in report.items():
            if isinstance(value, dict):
                print(f"{key}:")
                for sub_key, sub_value in value.items():
                    print(f"  - {sub_key}: {sub_value}")
            else:
                print(f"{key}: {value}")
        
    except Exception as e:
        print(f"❌ 성능 리포트 조회 실패: {str(e)}")
    
    # 최종 결과
    print(f"\n🎯 테스트 결과: {success_count}/{len(test_queries)} 성공")
    
    if success_count == len(test_queries):
        print("🎉 모든 테스트 성공! Hybrid 시스템이 정상 작동합니다!")
        return True
    else:
        print(f"⚠️ {len(test_queries) - success_count}개 테스트 실패")
        return False

if __name__ == "__main__":
    asyncio.run(test_hybrid_workflow())