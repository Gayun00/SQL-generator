"""
UserCommunicatorAgent 테스트 케이스
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import json
from agents.user_communicator_agent import UserCommunicatorAgent, create_user_communicator_agent
from agents.simple_base_agent import AgentMessage, MessageType, create_agent_config

async def test_user_initial_input():
    """사용자 초기 입력 테스트"""
    print("=== 테스트 1: 사용자 초기 입력 ===")
    
    agent = create_user_communicator_agent()
    
    # 무의미한 사용자 입력
    message = AgentMessage(
        id="test_1",
        type=MessageType.TASK,
        source="test",
        target="user_communicator",
        content={
            "step": "generate_question",
            "source": "user",
            "userInput": "ㅇㅇ"
        }
    )
    
    result = await agent.process_message(message)
    print(f"입력: {message.content['userInput']}")
    print(f"결과: {json.dumps(result.content, indent=2, ensure_ascii=False)}")
    print()

async def test_sql_generator_request():
    """SQL Generator 요청 테스트"""
    print("=== 테스트 2: SQL Generator 요청 ===")
    
    agent = create_user_communicator_agent()
    
    message = AgentMessage(
        id="test_2",
        type=MessageType.TASK,
        source="sql_generator",
        target="user_communicator",
        content={
            "step": "generate_question",
            "source": "sql_generator",
            "agentRequest": {
                "missingFields": ["date_range", "sort_order"],
                "reason": "날짜 범위와 정렬 방식이 명시되지 않음"
            }
        }
    )
    
    result = await agent.process_message(message)
    print(f"에이전트 요청: {message.content['agentRequest']}")
    print(f"결과: {json.dumps(result.content, indent=2, ensure_ascii=False)}")
    print()

async def test_finalize_step():
    """최종 정리 단계 테스트"""
    print("=== 테스트 3: 최종 정리 단계 ===")
    
    agent = create_user_communicator_agent()
    
    message = AgentMessage(
        id="test_3",
        type=MessageType.TASK,
        source="test",
        target="user_communicator",
        content={
            "step": "finalize",
            "source": "user",
            "userReply": "최근 3개월 데이터를 주문 횟수 많은 순으로 정렬해서 보여줘"
        }
    )
    
    result = await agent.process_message(message)
    print(f"사용자 응답: {message.content['userReply']}")
    print(f"결과: {json.dumps(result.content, indent=2, ensure_ascii=False)}")
    print()

async def test_data_explorer_request():
    """Data Explorer 요청 테스트"""
    print("=== 테스트 4: Data Explorer 요청 ===")
    
    agent = create_user_communicator_agent()
    
    message = AgentMessage(
        id="test_4",
        type=MessageType.TASK,
        source="data_explorer",
        target="user_communicator",
        content={
            "step": "generate_question",
            "source": "data_explorer",
            "agentRequest": {
                "missingFields": ["table_join_condition", "filter_criteria"],
                "reason": "테이블 조인 조건과 필터 조건이 불명확함"
            }
        }
    )
    
    result = await agent.process_message(message)
    print(f"데이터 탐색 요청: {message.content['agentRequest']}")
    print(f"결과: {json.dumps(result.content, indent=2, ensure_ascii=False)}")
    print()

async def main():
    """모든 테스트 실행"""
    print("UserCommunicatorAgent 테스트 시작\n")
    
    try:
        await test_user_initial_input()
        await test_sql_generator_request()
        await test_finalize_step()
        await test_data_explorer_request()
        
        print("모든 테스트 완료!")
        
    except Exception as e:
        print(f"테스트 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())