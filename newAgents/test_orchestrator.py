"""
Orchestrator Agent 테스트 스크립트
"""

import asyncio
import sys
import os

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from newAgents.orchestrator_agent import orchestrator_agent


async def test_orchestrator():
    """Orchestrator Agent 테스트"""
    print("🧪 Orchestrator Agent 테스트 시작")
    print("=" * 50)
    
    # 테스트 사용자 입력
    test_queries = [
        "최근 7일간 주문 데이터를 보여줘",
        "고객별 총 주문 금액을 알고 싶어",
        "이번 달 매출 통계를 조회해줘"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n🔍 테스트 {i}: {query}")
        print("-" * 30)
        
        try:
            # Orchestrator를 통한 요청 처리
            result = await orchestrator_agent.process_request(query)
            
            # 결과 출력
            if result.get("success"):
                print("✅ 처리 성공!")
                print(f"📝 사용자 입력: {result.get('user_input', '')}")
                print(f"🔍 스키마 정보: {len(result.get('schema_info', []))}개 테이블")
                print(f"⚡ SQL 쿼리: {result.get('sql_query', '')[:100]}...")
                
                exec_result = result.get('execution_result', {})
                if exec_result and exec_result.get('success'):
                    print(f"📊 실행 결과: {exec_result.get('returned_rows', 0)}개 행")
                else:
                    print(f"❌ 실행 실패: {exec_result.get('error', 'Unknown error') if exec_result else 'No execution result'}")
            else:
                print(f"❌ 처리 실패: {result.get('error', '알 수 없는 오류')}")
                
        except Exception as e:
            print(f"❌ 테스트 중 오류: {str(e)}")
        
        print("-" * 30)
    
    print("\n🧪 테스트 완료")


if __name__ == "__main__":
    # 비동기 테스트 실행
    asyncio.run(test_orchestrator())