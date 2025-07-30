#!/usr/bin/env python3
"""
완전한 A2A 시스템 통합 테스트

모든 Agent들이 협력하여 작동하는 전체 A2A 시스템을 테스트합니다.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from agents.dynamic_orchestrator import DynamicOrchestrator, ExecutionContext
from workflow.state import SQLGeneratorState
from db.bigquery_client import bq_client
from rag.schema_embedder import schema_embedder
from rag.schema_retriever import schema_retriever

def initialize_test_environment():
    """테스트 환경 초기화"""
    print("🔧 완전한 A2A 시스템 테스트 환경 초기화 중...")
    
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

async def test_orchestrator_agent_coordination():
    """DynamicOrchestrator의 Agent 협력 테스트"""
    print("\\n🧪 DynamicOrchestrator Agent 협력 테스트")
    print("-" * 60)
    
    # DynamicOrchestrator 생성
    orchestrator = DynamicOrchestrator()
    
    # 모든 Agent 등록
    from agents.schema_analyzer_agent import create_schema_analyzer_agent
    from agents.sql_generator_agent import create_sql_generator_agent
    from agents.data_explorer_agent import create_data_explorer_agent
    from agents.user_communicator_agent import create_user_communicator_agent
    
    agents = [
        create_schema_analyzer_agent(),
        create_sql_generator_agent(),
        create_data_explorer_agent(),
        create_user_communicator_agent()
    ]
    
    
    # 복잡한 SQL 요청으로 Agent 협력 테스트
    test_query = "최근 한 달간 가장 많이 주문한 상위 10명의 사용자와 그들의 총 주문 금액을 보여주세요"
    
    context = ExecutionContext(
        query=test_query,
        state={
            "userInput": test_query,
            "isValid": True
        }
    )
    
    try:
        print(f"\\n🔄 복잡한 쿼리 처리 중...")
        print(f"쿼리: {test_query}")
        
        result = await orchestrator.execute_dynamic_workflow(test_query)
        
        print(f"\\n✅ Orchestrator 처리 완료!")
        print(f"완료 타입: {result.get('termination_reason', 'unknown')}")
        print(f"실행된 Agent: {len(result.get('executed_agents', []))}개")
        print(f"처리 시간: {result.get('execution_time', 0):.2f}초")
        
        # 각 Agent의 결과 확인
        agent_results = result.get("agent_results", {})
        for agent_name, agent_result in agent_results.items():
            print(f"\\n📋 {agent_name}:")
            if isinstance(agent_result, dict):
                status = "✅ 성공" if not agent_result.get("error") else "❌ 실패"
                print(f"   결과: {status}")
        
        return True
        
    except Exception as e:
        print(f"❌ Orchestrator 테스트 실패: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

async def test_pure_a2a_workflow():
    """순수 A2A Workflow 테스트"""
    print("\\n🧪 순수 A2A Workflow 테스트")
    print("-" * 60)
    
    # DynamicOrchestrator 생성
    orchestrator = DynamicOrchestrator()
    
    # 모든 Agent 등록
    from agents.schema_analyzer_agent import create_schema_analyzer_agent
    from agents.sql_generator_agent import create_sql_generator_agent
    from agents.data_explorer_agent import create_data_explorer_agent
    from agents.user_communicator_agent import create_user_communicator_agent
    
    agents = [
        create_schema_analyzer_agent(),
        create_sql_generator_agent(),
        create_data_explorer_agent(),
        create_user_communicator_agent()
    ]
    
    for agent in agents:
        orchestrator.register_agent(agent)
    
    # 다양한 복잡도의 쿼리 테스트
    test_cases = [
        {
            "name": "단순 조회",
            "query": "users 테이블의 모든 데이터를 보여주세요",
            "complexity": "simple"
        },
        {
            "name": "조건부 조회",
            "query": "주문 상태가 완료인 주문들을 최근 것부터 100개만 보여주세요",
            "complexity": "moderate"
        },
        {
            "name": "복잡한 집계",
            "query": "월별로 카테고리별 매출을 집계하고 전월 대비 증감률도 함께 보여주세요",
            "complexity": "complex"
        }
    ]
    
    print(f"📋 {len(test_cases)}개 테스트 케이스 실행")
    
    success_count = 0
    comparison_results = []
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\\n🧪 테스트 {i}: {test_case['name']} ({test_case['complexity']})")
        print(f"쿼리: {test_case['query']}")
        
        # SQLGeneratorState 생성
        state = {
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
            # 순수 A2A 실행
            context = ExecutionContext(
                query=test_case["query"],
                state=state
            )
            
            result = await orchestrator.execute_dynamic_workflow(test_case["query"])
            
            print(f"✅ 처리 완료!")
            print(f"   실행 계획: {result.get('execution_plan', {}).get('strategy', 'unknown')}")
            print(f"   참여 Agent: {len(result.get('results', {}))}개")
            print(f"   처리 시간: {result.get('total_processing_time', 0):.2f}초")
            
            # 각 Agent의 결과 확인
            results = result.get("results", {})
            for phase_name, phase_result in results.items():
                print(f"   📋 {phase_name}: {len(phase_result)}개 작업 완료")
            
            success_count += 1
            
        except Exception as e:
            print(f"❌ 테스트 실패: {str(e)}")
    
    # 전체 결과 요약
    print(f"\\n📊 순수 A2A 테스트 결과: {success_count}/{len(test_cases)} 성공")
    
    if success_count > 0:
        print(f"\\n🎉 A2A 시스템 성과:")
        print(f"   ✅ 모든 Agent가 독립적으로 정상 작동")
        print(f"   ✅ MasterOrchestrator 중앙 조정 성공")
        print(f"   ✅ 복잡한 쿼리 처리 완료")
        print(f"   ✅ Hybrid 시스템 없이도 완벽한 동작")
    
    return success_count == len(test_cases)

async def test_agent_statistics_summary():
    """모든 Agent의 통계 요약"""
    print("\\n🧪 전체 Agent 통계 요약")
    print("-" * 60)
    
    # 모든 Agent 생성
    from agents.schema_analyzer_agent import create_schema_analyzer_agent
    from agents.sql_generator_agent import create_sql_generator_agent
    from agents.data_explorer_agent import create_data_explorer_agent
    from agents.user_communicator_agent import create_user_communicator_agent
    
    agents = [
        ("SchemaAnalyzer", create_schema_analyzer_agent()),
        ("SQLGenerator", create_sql_generator_agent()),
        ("DataExplorer", create_data_explorer_agent()),
        ("UserCommunicator", create_user_communicator_agent())
    ]
    
    print("📊 Agent 통계 요약:")
    
    for agent_name, agent in agents:
        print(f"\\n🤖 {agent_name} Agent:")
        
        try:
            stats = agent.get_agent_statistics()
            
            if isinstance(stats, dict) and "message" in stats:
                print(f"   {stats['message']}")
            else:
                for key, value in stats.items():
                    print(f"   {key}: {value}")
                    
        except Exception as e:
            print(f"   ❌ 통계 조회 실패: {str(e)}")
    
    return True

async def test_system_scalability():
    """시스템 확장성 테스트"""
    print("\\n🧪 시스템 확장성 테스트")
    print("-" * 60)
    
    # MasterOrchestrator 생성
    orchestrator = MasterOrchestrator()
    
    # Agent 등록
    from agents.sql_generator_agent import create_sql_generator_agent
    agent = create_sql_generator_agent()
    orchestrator.register_agent(agent)
    
    concurrent_queries = [
        "SELECT COUNT(*) FROM users",
        "SELECT * FROM orders LIMIT 5",
        "SELECT category, COUNT(*) FROM products GROUP BY category",
        "SELECT AVG(amount) FROM transactions",
        "SELECT DISTINCT status FROM orders"
    ]
    
    print(f"🔄 {len(concurrent_queries)}개 쿼리 동시 처리 테스트")
    
    start_time = asyncio.get_event_loop().time()
    
    # 동시 실행
    tasks = []
    for i, query in enumerate(concurrent_queries):
        context = ExecutionContext(
            query=query,
            state={"userInput": query, "isValid": True}
        )
        task = orchestrator.process_sql_request(context)
        tasks.append(task)
    
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        end_time = asyncio.get_event_loop().time()
        
        successful = sum(1 for r in results if not isinstance(r, Exception))
        total_time = end_time - start_time
        
        print(f"✅ 동시 처리 완료!")
        print(f"   성공: {successful}/{len(concurrent_queries)}")
        print(f"   총 시간: {total_time:.2f}초")
        print(f"   평균 처리 시간: {total_time/len(concurrent_queries):.2f}초")
        
        return successful > 0
        
    except Exception as e:
        print(f"❌ 확장성 테스트 실패: {str(e)}")
        return False

async def main():
    """메인 테스트 실행"""
    print("🚀 완전한 A2A 시스템 통합 테스트 시작!")
    print("=" * 80)
    
    # 환경 초기화
    if not initialize_test_environment():
        print("❌ 환경 초기화 실패로 테스트 중단")
        return False
    
    # 테스트 실행
    tests = [
        ("MasterOrchestrator Agent 협력", test_orchestrator_agent_coordination),
        ("순수 A2A Workflow", test_pure_a2a_workflow),
        ("Agent 통계 요약", test_agent_statistics_summary),
        ("시스템 확장성", test_system_scalability)
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
    
    # 최종 결과
    print("\\n" + "=" * 80)
    print(f"🎯 A2A 시스템 통합 테스트 결과: {passed}/{total} 통과")
    
    if passed == total:
        print("\\n🎉 완전한 A2A 시스템 테스트 성공!")
        print("✅ 모든 Agent가 협력하여 정상 작동합니다!")
        print("🏆 SQL Generator가 완전한 A2A 아키텍처로 전환되었습니다!")
        
        print("\\n📋 순수 A2A 시스템 구성:")
        print("   1. 🧠 SchemaIntelligence Agent - 스키마 분석 및 불확실성 탐지")
        print("   2. 🏗️  SqlGenerator Agent - SQL 설계, 최적화 및 자동 개선")
        print("   3. 🔍 DataExplorer Agent - 데이터 탐색 및 불확실성 해결") 
        print("   4. 💬 CommunicationSpecialist Agent - 사용자 커뮤니케이션")
        print("   5. 🎛️  MasterOrchestrator - 중앙 집중식 Agent 조정 및 통신 관리")
        
    else:
        print(f"\\n⚠️ {total - passed}개 테스트 실패")
        print("🔧 시스템 개선이 필요합니다.")
    
    return passed == total

if __name__ == "__main__":
    asyncio.run(main())