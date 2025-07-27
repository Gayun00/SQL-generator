#!/usr/bin/env python3
"""
개선방안 즉시 실행 기능 테스트

QueryArchitect Agent의 SQL 실행 실패시 개선방안 즉시 적용 기능을 테스트합니다.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from agents.query_architect_agent import create_query_architect_agent
from agents.base_agent import AgentMessage, MessageType
from db.bigquery_client import bq_client
from rag.schema_embedder import schema_embedder
from rag.schema_retriever import schema_retriever

def initialize_test_environment():
    """테스트 환경 초기화"""
    print("🔧 개선방안 실행 테스트 환경 초기화 중...")
    
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

async def test_improvement_execution():
    """개선방안 즉시 실행 테스트"""
    print("\\n🧪 개선방안 즉시 실행 테스트")
    print("-" * 60)
    
    # Agent 생성
    agent = create_query_architect_agent()
    
    # 실패할 것으로 예상되는 SQL 테스트 케이스들
    test_cases = [
        {
            "name": "컬럼명 오류",
            "sql_query": "SELECT * FROM us_plus.users WHERE status = 'active' LIMIT 10",
            "original_query": "활성 사용자들을 조회해주세요",
            "expected_improvement": "column_name"
        },
        {
            "name": "데이터 타입 오류", 
            "sql_query": "SELECT * FROM us_plus.orders WHERE createdAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) LIMIT 10",
            "original_query": "최근 일주일간 주문을 조회해주세요",
            "expected_improvement": "data_type"
        }
    ]
    
    print(f"📋 {len(test_cases)}개 테스트 케이스 실행")
    
    success_count = 0
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\\n🧪 테스트 {i}: {test_case['name']}")
        print(f"SQL: {test_case['sql_query']}")
        print(f"원본 요청: {test_case['original_query']}")
        
        # 메시지 생성
        message = AgentMessage(
            sender="test",
            receiver="query_architect",
            message_type=MessageType.REQUEST,
            content={
                "task_type": "execute_with_improvements",
                "sql_query": test_case["sql_query"],
                "original_query": test_case["original_query"]
            }
        )
        
        try:
            # Agent 실행 (개선방안 적용)
            print("🔄 개선방안 포함 실행 중...")
            response = await agent.process_message(message)
            
            if response.message_type == MessageType.ERROR:
                print(f"❌ Agent 오류: {response.content.get('error_message', 'Unknown error')}")
                continue
            
            # 결과 분석
            result = response.content
            execution_type = result.get("execution_type", "unknown")
            success = result.get("success", False)
            processing_time = result.get("processing_time", 0)
            improvements_applied = result.get("improvements_applied", False)
            
            print(f"\\n✅ 실행 완료 ({processing_time:.2f}초)")
            print(f"   실행 타입: {execution_type}")
            print(f"   성공 여부: {'성공' if success else '실패'}")
            print(f"   개선방안 적용: {'예' if improvements_applied else '아니오'}")
            
            if improvements_applied and result.get("improvement_details"):
                details = result["improvement_details"]
                print(f"   개선 타입: {details.get('type', 'unknown')}")
                print(f"   개선 설명: {details.get('description', 'N/A')}")
                print(f"   신뢰도: {details.get('confidence', 0):.2f}")
                print(f"   변경사항: {', '.join(details.get('changes_made', []))}")
            
            if success:
                query_result = result.get("query_result", {})
                if query_result.get("success"):
                    print(f"   📊 쿼리 결과: {query_result.get('returned_rows', 0)}개 행")
                success_count += 1
                print("✅ 테스트 성공")
            else:
                print(f"   ❌ 오류: {result.get('error', 'Unknown error')}")
            
        except Exception as e:
            print(f"❌ 테스트 실패: {str(e)}")
            import traceback
            traceback.print_exc()
    
    print(f"\\n📊 개선방안 실행 테스트 결과: {success_count}/{len(test_cases)} 성공")
    return success_count > 0

async def test_improvement_generation_only():
    """개선방안 생성만 테스트 (실행 X)"""
    print("\\n🧪 개선방안 생성 테스트")
    print("-" * 60)
    
    # Agent 생성
    agent = create_query_architect_agent()
    
    # 간단한 컬럼명 오류 SQL
    sql_query = "SELECT * FROM us_plus.users WHERE status = 'active'"
    error_message = "400 Unrecognized name: status at [1:39]; Did you mean orderStatus?"
    original_query = "활성 사용자 조회"
    
    print(f"SQL: {sql_query}")
    print(f"오류: {error_message}")
    
    try:
        # 개선방안 생성 테스트
        improvements = await agent._generate_sql_improvements(sql_query, error_message, original_query)
        
        print(f"\\n🛠️ 생성된 개선방안: {len(improvements)}개")
        
        for i, improvement in enumerate(improvements, 1):
            print(f"\\n개선방안 {i}:")
            print(f"   타입: {improvement.get('issue_type', 'unknown')}")
            print(f"   설명: {improvement.get('description', 'N/A')}")
            print(f"   신뢰도: {improvement.get('confidence', 0):.2f}")
            print(f"   개선된 SQL: {improvement.get('improved_sql', 'N/A')[:100]}...")
            if improvement.get('changes_made'):
                print(f"   변경사항: {', '.join(improvement['changes_made'])}")
        
        return len(improvements) > 0
        
    except Exception as e:
        print(f"❌ 개선방안 생성 실패: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """메인 테스트 실행"""
    print("🚀 개선방안 즉시 실행 기능 테스트 시작!")
    print("=" * 70)
    
    # 환경 초기화
    if not initialize_test_environment():
        print("❌ 환경 초기화 실패로 테스트 중단")
        return False
    
    # 테스트 실행
    tests = [
        ("개선방안 생성", test_improvement_generation_only),
        ("개선방안 즉시 실행", test_improvement_execution)
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
    print(f"🎯 개선방안 즉시 실행 테스트 결과: {passed}/{total} 통과")
    
    if passed == total:
        print("🎉 개선방안 즉시 실행 기능이 성공적으로 구현되었습니다!")
        print("✅ QueryArchitect Agent가 SQL 실패시 자동으로 개선방안을 제안하고 실행합니다!")
    else:
        print(f"⚠️ {total - passed}개 테스트 실패")
        print("🔧 기능 개선이 필요합니다.")
    
    return passed == total

if __name__ == "__main__":
    asyncio.run(main())