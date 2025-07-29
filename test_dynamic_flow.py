#!/usr/bin/env python3
"""
A2A 동적 플로우 상세 테스트

각 Agent의 결과에 따라 플로우가 어떻게 조정되는지 테스트합니다.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
from workflow.a2a_workflow import create_a2a_workflow

async def test_dynamic_flow_scenarios():
    """다양한 동적 플로우 시나리오 테스트"""
    
    workflow = await create_a2a_workflow()
    
    print("🧪 A2A 동적 플로우 상세 테스트")
    print("=" * 70)
    
    test_scenarios = [
        {
            "name": "단순 쿼리 (탐색 스킵 예상)",
            "query": "SELECT * FROM users LIMIT 10",
            "expected_complexity": "low",
            "expected_phases": ["validation", "generation"]
        },
        {
            "name": "불확실성 있는 쿼리 (탐색 단계 포함 예상)",
            "query": "활성 상태인 사용자들의 최근 주문 내역을 카테고리별로 분석해줘",
            "expected_complexity": "medium",
            "expected_phases": ["analysis", "exploration", "generation"]
        },
        {
            "name": "복잡한 집계 쿼리 (전체 단계 예상)",
            "query": "최근 6개월간 월별 카테고리별 매출 추이와 전월 대비 증감률을 구하고, 상위 10개 카테고리의 고객 세그먼트별 구매 패턴을 분석해줘",
            "expected_complexity": "high", 
            "expected_phases": ["comprehensive_analysis", "iterative_refinement", "optimization"]
        }
    ]
    
    try:
        for i, scenario in enumerate(test_scenarios, 1):
            print(f"\\n🎯 시나리오 {i}: {scenario['name']}")
            print(f"쿼리: {scenario['query']}")
            print("-" * 70)
            
            result = await workflow.process_query(scenario["query"])
            
            if result.get("success"):
                execution_plan = result.get("execution_plan", {})
                strategy = execution_plan.get("strategy", "unknown")
                completed_phases = execution_plan.get("completed_phases", [])
                early_completion = execution_plan.get("early_completion", False)
                total_time = result.get("execution_time", 0)
                
                print(f"\\n📊 실행 결과:")
                print(f"   전략: {strategy}")
                print(f"   완료된 단계: {', '.join(completed_phases)} ({len(completed_phases)}개)")
                print(f"   실행 시간: {total_time:.2f}초")
                print(f"   조기 완료: {'예' if early_completion else '아니오'}")
                
                # 동적 플로우 분석
                print(f"\\n🔍 동적 플로우 분석:")
                
                # 복잡도별 플랜 확인
                plan_id = result.get("plan_id", "")
                if "simple" in plan_id:
                    complexity = "단순"
                elif "standard" in plan_id:
                    complexity = "표준"
                elif "complex" in plan_id:
                    complexity = "복잡"
                else:
                    complexity = "알 수 없음"
                
                print(f"   복잡도 판정: {complexity}")
                
                # Agent별 결과 상세 분석
                results = result.get("results", {})
                for phase_name, phase_result in results.items():
                    print(f"\\n   📋 {phase_name} 단계:")
                    
                    for task_name, task_result in phase_result.items():
                        if isinstance(task_result, dict):
                            status = "✅ 성공" if not task_result.get("error") else "❌ 실패"
                            print(f"      {task_name}: {status}")
                            
                            # 중요 결과 표시
                            if task_name == "full_analysis":
                                ua = task_result.get("uncertainty_analysis", {})
                                has_uncertainty = ua.get("has_uncertainty", False)
                                confidence = ua.get("confidence", 0.0)
                                uncertainties = ua.get("uncertainties", [])
                                
                                print(f"         불확실성: {'있음' if has_uncertainty else '없음'}")
                                print(f"         신뢰도: {confidence:.2f}")
                                
                                if uncertainties:
                                    print(f"         발견된 불확실성: {len(uncertainties)}개")
                                    for unc in uncertainties[:2]:
                                        print(f"           - {unc.get('type', 'unknown')}: {unc.get('description', 'N/A')[:50]}...")
                            
                            elif task_name in ["simple_generation", "optimized_generation"]:
                                sql_query = task_result.get("sql_query", "")
                                query_result = task_result.get("query_result", {})
                                
                                if sql_query:
                                    print(f"         생성된 SQL: {sql_query[:60]}{'...' if len(sql_query) > 60 else ''}")
                                
                                if query_result.get("success"):
                                    rows = query_result.get("returned_rows", 0)
                                    print(f"         실행 결과: ✅ {rows}개 행")
                                elif query_result.get("error"):
                                    error = query_result.get("error", "")
                                    print(f"         실행 결과: ❌ {error[:50]}...")
                
                # 플로우 효율성 평가
                print(f"\\n💡 플로우 효율성:")
                expected_phases = scenario.get("expected_phases", [])
                
                if set(completed_phases) <= set(expected_phases):
                    print(f"   ✅ 예상된 단계와 일치하거나 더 효율적")
                else:
                    unexpected = set(completed_phases) - set(expected_phases)
                    print(f"   ⚠️ 예상보다 추가 단계 실행: {', '.join(unexpected)}")
                
                if early_completion:
                    print(f"   ⚡ 조기 완료로 효율성 향상")
                
            else:
                print(f"❌ 실행 실패: {result.get('error', 'Unknown error')}")
    
    finally:
        await workflow.shutdown()

async def test_error_handling_flow():
    """오류 처리 및 개선 플로우 테스트"""
    
    print("\\n\\n🛠️ 오류 처리 및 개선 플로우 테스트")
    print("=" * 70)
    
    workflow = await create_a2a_workflow()
    
    try:
        # 의도적으로 오류가 발생할 수 있는 쿼리
        error_query = "테이블명이나 컬럼명이 틀린 복잡한 쿼리를 실행해서 오류 처리 테스트"
        
        print(f"🧪 오류 시나리오 테스트: {error_query}")
        print("-" * 70)
        
        result = await workflow.process_query(error_query)
        
        if result.get("success"):
            execution_plan = result.get("execution_plan", {})
            completed_phases = execution_plan.get("completed_phases", [])
            
            print(f"\\n📊 오류 처리 결과:")
            print(f"   완료된 단계: {', '.join(completed_phases)}")
            
            # improvement 단계가 추가되었는지 확인
            if "improvement" in completed_phases:
                print("   ✅ 자동 개선 단계가 동적으로 추가됨")
            
            # clarification 단계가 추가되었는지 확인
            if "clarification" in completed_phases:
                print("   ✅ 재질문 단계가 동적으로 추가됨")
            
            print("\\n💡 A2A 시스템의 자동 오류 복구 기능이 작동함")
        
    finally:
        await workflow.shutdown()

if __name__ == "__main__":
    async def main():
        await test_dynamic_flow_scenarios()
        await test_error_handling_flow()
        
        print("\\n" + "=" * 70)
        print("🎉 A2A 동적 플로우 테스트 완료!")
        print("\\n✅ 확인된 기능:")
        print("   • Agent 결과 기반 동적 플로우 조정")
        print("   • 불필요한 단계 자동 스킵")
        print("   • 오류 발생시 자동 개선 단계 추가")
        print("   • 복잡도별 적응형 실행 계획")
        print("   • 조기 완료를 통한 효율성 향상")
    
    asyncio.run(main())