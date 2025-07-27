#!/usr/bin/env python3
"""
Phase 4 Agents Test - DataInvestigator & CommunicationSpecialist Agent 테스트

새로 구현된 두 Agent의 기능을 종합적으로 테스트합니다.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from agents.data_investigator_agent import create_data_investigator_agent
from agents.communication_specialist_agent import create_communication_specialist_agent
from agents.base_agent import AgentMessage, MessageType
from db.bigquery_client import bq_client
from rag.schema_embedder import schema_embedder
from rag.schema_retriever import schema_retriever

def initialize_test_environment():
    """테스트 환경 초기화"""
    print("🔧 Phase 4 Agents 테스트 환경 초기화 중...")
    
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

async def test_data_investigator():
    """DataInvestigator Agent 테스트"""
    print("\\n🧪 DataInvestigator Agent 테스트")
    print("-" * 50)
    
    # Agent 생성
    agent = create_data_investigator_agent()
    
    # 테스트 케이스: 불확실성 탐색
    test_uncertainties = [
        {
            "type": "column_values",
            "description": "users 테이블의 status 컬럼에 어떤 값들이 있는지 불확실",
            "exploration_query": "SELECT DISTINCT status, COUNT(*) as count FROM us_plus.users GROUP BY status LIMIT 10"
        },
        {
            "type": "data_range",
            "description": "orders 테이블의 데이터 범위가 불확실",
            "exploration_query": "SELECT MIN(created_at) as earliest, MAX(created_at) as latest, COUNT(*) as total FROM us_plus.orders LIMIT 1"
        }
    ]
    
    print(f"📋 {len(test_uncertainties)}개 불확실성 탐색 테스트")
    
    # 메시지 생성
    message = AgentMessage(
        sender="test",
        receiver="data_investigator",
        message_type=MessageType.REQUEST,
        content={
            "task_type": "uncertainty_exploration",
            "uncertainties": test_uncertainties,
            "query": "사용자별 주문 상태를 확인하고 싶어요"
        }
    )
    
    try:
        # Agent 실행
        print("🔄 DataInvestigator Agent 실행 중...")
        response = await agent.process_message(message)
        
        if response.message_type == MessageType.ERROR:
            print(f"❌ Agent 오류: {response.content.get('error_message', 'Unknown error')}")
            return False
        
        # 결과 분석
        result = response.content
        exploration_type = result.get("exploration_type", "unknown")
        executed_queries = result.get("executed_queries", 0)
        results = result.get("results", [])
        insights = result.get("insights", [])
        summary = result.get("summary", "")
        processing_time = result.get("processing_time", 0)
        
        print(f"✅ 탐색 완료 ({processing_time:.2f}초)")
        print(f"   탐색 타입: {exploration_type}")
        print(f"   실행된 쿼리: {executed_queries}개")
        print(f"   성공한 탐색: {len([r for r in results if r.get('success', False)])}개")
        print(f"   발견된 인사이트: {len(insights)}개")
        print(f"   요약: {summary}")
        
        # 인사이트 출력
        if insights:
            print("💡 주요 인사이트:")
            for i, insight in enumerate(insights[:3], 1):
                print(f"   {i}. {insight}")
        
        # 탐색 결과 출력
        for i, result_item in enumerate(results, 1):
            print(f"\\n🔍 탐색 {i}: {result_item.get('uncertainty_type', 'unknown')}")
            if result_item.get("success"):
                print(f"   ✅ 성공: {result_item.get('total_rows', 0)}개 결과")
                print(f"   💡 {result_item.get('insight', 'N/A')}")
            else:
                print(f"   ❌ 실패: {result_item.get('error', 'Unknown error')}")
        
        return True
        
    except Exception as e:
        print(f"❌ DataInvestigator 테스트 실패: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

async def test_communication_specialist():
    """CommunicationSpecialist Agent 테스트"""
    print("\\n🧪 CommunicationSpecialist Agent 테스트")
    print("-" * 50)
    
    # Agent 생성
    agent = create_communication_specialist_agent()
    
    # 테스트 케이스 1: 입력 검증
    print("\\n📋 테스트 1: 사용자 입력 검증")
    
    validation_tests = [
        {
            "input": "users 테이블에서 활성 사용자 수를 알려주세요",
            "expected_valid": True
        },
        {
            "input": "안녕하세요",
            "expected_valid": False
        }
    ]
    
    validation_success = 0
    
    for i, test_case in enumerate(validation_tests, 1):
        print(f"\\n   검증 {i}: '{test_case['input']}'")
        
        message = AgentMessage(
            sender="test",
            receiver="communication_specialist",
            message_type=MessageType.REQUEST,
            content={
                "task_type": "validate_input",
                "user_input": test_case["input"]
            }
        )
        
        try:
            response = await agent.process_message(message)
            
            if response.message_type == MessageType.ERROR:
                print(f"   ❌ 검증 오류: {response.content.get('error_message', 'Unknown error')}")
                continue
            
            result = response.content
            is_valid = result.get("is_valid", False)
            confidence = result.get("confidence", 0.0)
            reason = result.get("reason", "")
            
            print(f"   결과: {'유효' if is_valid else '무효'} (신뢰도: {confidence:.2f})")
            print(f"   이유: {reason}")
            
            # 기대값과 비교
            if is_valid == test_case["expected_valid"]:
                print("   ✅ 검증 성공")
                validation_success += 1
            else:
                print("   ⚠️ 검증 결과 불일치")
                
        except Exception as e:
            print(f"   ❌ 검증 실패: {str(e)}")
    
    print(f"\\n📊 입력 검증 테스트: {validation_success}/{len(validation_tests)} 성공")
    
    # 테스트 케이스 2: 재질문 생성
    print("\\n📋 테스트 2: 재질문 생성")
    
    unresolved_uncertainties = [
        {
            "uncertainty_type": "column_values",
            "description": "status 컬럼의 정확한 값들을 모르겠습니다",
            "error": "탐색 쿼리 실행 실패"
        },
        {
            "uncertainty_type": "data_range",
            "description": "최근 일주일의 기준점이 불명확합니다",
            "error": "날짜 범위 모호"
        }
    ]
    
    message = AgentMessage(
        sender="test",
        receiver="communication_specialist",
        message_type=MessageType.REQUEST,
        content={
            "task_type": "generate_clarification",
            "unresolved_uncertainties": unresolved_uncertainties,
            "original_query": "최근 활성 사용자들의 주문 현황을 보여주세요",
            "exploration_results": {
                "insights": ["users 테이블에 10,000개 레코드 확인", "orders 테이블에서 최근 데이터 부족"]
            }
        }
    )
    
    try:
        print("🔄 재질문 생성 중...")
        response = await agent.process_message(message)
        
        if response.message_type == MessageType.ERROR:
            print(f"❌ 재질문 생성 오류: {response.content.get('error_message', 'Unknown error')}")
            return False
        
        result = response.content
        needs_clarification = result.get("needs_clarification", False)
        questions = result.get("questions", [])
        summary = result.get("summary", "")
        confidence = result.get("confidence", 0.0)
        processing_time = result.get("processing_time", 0)
        
        print(f"✅ 재질문 생성 완료 ({processing_time:.2f}초)")
        print(f"   재질문 필요: {needs_clarification}")
        print(f"   생성된 질문: {len(questions)}개")
        print(f"   신뢰도: {confidence:.2f}")
        print(f"   요약: {summary}")
        
        # 생성된 질문들 출력
        if questions:
            print("\\n❓ 생성된 재질문:")
            for i, question in enumerate(questions, 1):
                q_text = question.get("question", "")
                q_context = question.get("context", "")
                q_examples = question.get("examples", [])
                q_priority = question.get("priority", "medium")
                
                print(f"   {i}. [{q_priority}] {q_text}")
                print(f"      배경: {q_context}")
                if q_examples:
                    print(f"      예시: {', '.join(q_examples[:2])}")
        
        return len(questions) > 0
        
    except Exception as e:
        print(f"❌ CommunicationSpecialist 테스트 실패: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

async def test_agent_statistics():
    """Agent 통계 테스트"""
    print("\\n🧪 Agent 통계 테스트")
    print("-" * 50)
    
    # 두 Agent 생성
    data_agent = create_data_investigator_agent()
    comm_agent = create_communication_specialist_agent()
    
    agents = [
        ("DataInvestigator", data_agent),
        ("CommunicationSpecialist", comm_agent)
    ]
    
    for agent_name, agent in agents:
        print(f"\\n📈 {agent_name} Agent 통계:")
        
        try:
            stats = agent.get_agent_statistics()
            
            for key, value in stats.items():
                print(f"   {key}: {value}")
                
        except Exception as e:
            print(f"   ❌ 통계 조회 실패: {str(e)}")
    
    return True

async def main():
    """메인 테스트 실행"""
    print("🚀 Phase 4 Agents 종합 테스트 시작!")
    print("=" * 70)
    
    # 환경 초기화
    if not initialize_test_environment():
        print("❌ 환경 초기화 실패로 테스트 중단")
        return False
    
    # 테스트 실행
    tests = [
        ("DataInvestigator Agent", test_data_investigator),
        ("CommunicationSpecialist Agent", test_communication_specialist),
        ("Agent 통계", test_agent_statistics)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\\n🧪 {test_name}")
        print("=" * 70)
        
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
    print("\\n" + "=" * 70)
    print(f"🎯 Phase 4 Agents 테스트 결과: {passed}/{total} 통과")
    
    if passed == total:
        print("🎉 모든 Phase 4 Agent 테스트 통과!")
        print("✅ DataInvestigator & CommunicationSpecialist Agents가 성공적으로 구현되었습니다!")
    else:
        print(f"⚠️ {total - passed}개 테스트 실패")
        print("🔧 Agent 개선이 필요합니다.")
    
    return passed == total

if __name__ == "__main__":
    asyncio.run(main())