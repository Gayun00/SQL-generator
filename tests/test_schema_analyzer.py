# -*- coding: utf-8 -*-
"""
SchemaAnalyzerAgent 테스트 케이스
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import json
from unittest.mock import Mock, patch, MagicMock
from agents.schema_analyzer_agent import SchemaAnalyzerAgent, create_schema_analyzer_agent
from agents.simple_base_agent import AgentMessage, MessageType, create_agent_config

# 모의 테이블 데이터
MOCK_TABLE_DATA = [
    {
        'table_name': 'users',
        'description': '사용자 정보 테이블',
        'columns': [
            {'name': 'user_id', 'type': 'STRING', 'description': '사용자 ID'},
            {'name': 'email', 'type': 'STRING', 'description': '사용자 이메일'},
            {'name': 'created_at', 'type': 'TIMESTAMP', 'description': '계정 생성일'},
            {'name': 'status', 'type': 'STRING', 'description': '계정 상태'}
        ]
    },
    {
        'table_name': 'orders',
        'description': '주문 정보 테이블',
        'columns': [
            {'name': 'order_id', 'type': 'STRING', 'description': '주문 ID'},
            {'name': 'user_id', 'type': 'STRING', 'description': '주문한 사용자 ID'},
            {'name': 'order_date', 'type': 'TIMESTAMP', 'description': '주문 날짜'},
            {'name': 'total_amount', 'type': 'FLOAT64', 'description': '주문 총액'},
            {'name': 'status', 'type': 'STRING', 'description': '주문 상태'}
        ]
    }
]

async def test_valid_user_input():
    """유효한 사용자 입력 테스트"""
    print("=== 테스트 1: 유효한 사용자 입력 ===")
    
    agent = create_schema_analyzer_agent()
    
    # schema_retriever를 모킹
    with patch('agents.schema_analyzer_agent.schema_retriever') as mock_retriever:
        # 벡터스토어가 이미 초기화되어 있다고 가정
        mock_retriever.vectorstore = True
        mock_retriever.get_relevant_tables_with_threshold.return_value = MOCK_TABLE_DATA
        
        # 테스트용 메시지
        message = AgentMessage(
            id="test_1",
            type=MessageType.TASK,
            source="test",
            target="schema_analyzer",
            content={
                "userInput": "최근 3개월 동안 주문한 사용자들의 정보를 보여줘"
            }
        )
        
        result = await agent.process_message(message)
        print(f"입력: {message.content['userInput']}")
        print(f"결과: {json.dumps(result.content, indent=2, ensure_ascii=False)}")
        print()
        
        # 검증
        assert result.content['analysis_type'] in ['schema_context', 'needs_more_info']
        if result.content['analysis_type'] == 'schema_context':
            assert 'relevantTables' in result.content
            assert 'relevantFields' in result.content

async def test_empty_user_input():
    """빈 사용자 입력 테스트"""
    print("=== 테스트 2: 빈 사용자 입력 ===")
    
    agent = create_schema_analyzer_agent()
    
    message = AgentMessage(
        id="test_2",
        type=MessageType.TASK,
        source="test",
        target="schema_analyzer",
        content={
            "userInput": ""
        }
    )
    
    result = await agent.process_message(message)
    print(f"입력: '{message.content['userInput']}'")
    print(f"결과: {json.dumps(result.content, indent=2, ensure_ascii=False)}")
    print()
    
    # 검증
    assert result.content['analysis_type'] == 'needs_more_info'
    assert 'missingInfoDescription' in result.content
    assert 'followUpQuestions' in result.content

async def test_schema_retriever_not_initialized():
    """스키마 검색기 미초기화 테스트"""
    print("=== 테스트 3: 스키마 검색기 미초기화 ===")
    
    agent = create_schema_analyzer_agent()
    
    # schema_retriever를 모킹 - 초기화되지 않은 상태
    with patch('agents.schema_analyzer_agent.schema_retriever') as mock_retriever:
        mock_retriever.vectorstore = None
        mock_retriever.initialize.return_value = False
        
        message = AgentMessage(
            id="test_3",
            type=MessageType.TASK,
            source="test",
            target="schema_analyzer",
            content={
                "userInput": "사용자 정보를 조회하고 싶어"
            }
        )
        
        result = await agent.process_message(message)
        print(f"입력: {message.content['userInput']}")
        print(f"결과: {json.dumps(result.content, indent=2, ensure_ascii=False)}")
        print()
        
        # 검증
        assert result.content['analysis_type'] == 'needs_more_info'
        assert '스키마 검색 시스템' in result.content['missingInfoDescription']

async def test_no_relevant_schemas_found():
    """관련 스키마를 찾지 못한 경우 테스트"""
    print("=== 테스트 4: 관련 스키마를 찾지 못한 경우 ===")
    
    agent = create_schema_analyzer_agent()
    
    # schema_retriever를 모킹 - 빈 결과 반환 (두 번 모두)
    with patch('agents.schema_analyzer_agent.schema_retriever') as mock_retriever:
        mock_retriever.vectorstore = True
        mock_retriever.get_relevant_tables_with_threshold.return_value = []
        
        message = AgentMessage(
            id="test_4",
            type=MessageType.TASK,
            source="test",
            target="schema_analyzer",
            content={
                "userInput": "알 수 없는 데이터에 대한 쿼리"
            }
        )
        
        result = await agent.process_message(message)
        print(f"입력: {message.content['userInput']}")
        print(f"결과: {json.dumps(result.content, indent=2, ensure_ascii=False)}")
        print()
        
        # 검증 - 새로운 로직에서는 fallback 검색을 먼저 시도하므로 메시지가 달라질 수 있음
        assert result.content['analysis_type'] == 'needs_more_info'
        description = result.content['missingInfoDescription']
        assert any(keyword in description for keyword in [
            '관련된 스키마 정보를 찾을 수 없습니다', 
            '충분히 관련된 스키마를 찾지 못했습니다',
            '유사도 임계값'
        ])

async def test_schema_context_analysis():
    """스키마 컨텍스트 분석 테스트"""
    print("=== 테스트 5: 스키마 컨텍스트 분석 ===")
    
    agent = create_schema_analyzer_agent()
    
    # LLM 응답 모킹
    mock_llm_response = json.dumps({
        "has_sufficient_info": True,
        "relevant_tables": ["users", "orders"],
        "relevant_fields": {
            "users": ["user_id", "email", "created_at"],
            "orders": ["order_id", "user_id", "order_date", "total_amount"]
        },
        "suggested_joins": [
            {"from": "users", "to": "orders", "condition": "users.user_id = orders.user_id"}
        ],
        "natural_description": "사용자와 주문 정보를 조인하여 조회하는 쿼리"
    })
    
    with patch('agents.schema_analyzer_agent.schema_retriever') as mock_retriever:
        mock_retriever.vectorstore = True
        mock_retriever.get_relevant_tables_with_threshold.return_value = MOCK_TABLE_DATA
        
        # send_llm_request 메서드 모킹
        with patch.object(agent, 'send_llm_request', return_value=mock_llm_response):
            message = AgentMessage(
                id="test_5",
                type=MessageType.TASK,
                source="test",
                target="schema_analyzer",
                content={
                    "userInput": "사용자별 주문 통계를 보고 싶어"
                }
            )
            
            result = await agent.process_message(message)
            print(f"입력: {message.content['userInput']}")
            print(f"결과: {json.dumps(result.content, indent=2, ensure_ascii=False)}")
            print()
            
            # 검증
            assert result.content['analysis_type'] == 'schema_context'
            assert 'users' in result.content['relevantTables']
            assert 'orders' in result.content['relevantTables']
            assert 'relevantFields' in result.content
            assert 'joins' in result.content

async def test_needs_more_info_analysis():
    """추가 정보 필요 분석 테스트"""
    print("=== 테스트 6: 추가 정보 필요 분석 ===")
    
    agent = create_schema_analyzer_agent()
    
    # LLM 응답 모킹 - 추가 정보 필요
    mock_llm_response = json.dumps({
        "has_sufficient_info": False,
        "missing_info": {
            "description": "날짜 범위와 집계 방식이 명시되지 않음",
            "questions": [
                "어떤 기간의 데이터를 조회하고 싶으신가요?",
                "어떤 방식으로 데이터를 집계하고 싶으신가요?"
            ]
        }
    })
    
    with patch('agents.schema_analyzer_agent.schema_retriever') as mock_retriever:
        mock_retriever.vectorstore = True
        mock_retriever.get_relevant_tables_with_threshold.return_value = MOCK_TABLE_DATA
        
        # send_llm_request 메서드 모킹
        with patch.object(agent, 'send_llm_request', return_value=mock_llm_response):
            message = AgentMessage(
                id="test_6",
                type=MessageType.TASK,
                source="test",
                target="schema_analyzer",
                content={
                    "userInput": "통계를 보여줘"
                }
            )
            
            result = await agent.process_message(message)
            print(f"입력: {message.content['userInput']}")
            print(f"결과: {json.dumps(result.content, indent=2, ensure_ascii=False)}")
            print()
            
            # 검증
            assert result.content['analysis_type'] == 'needs_more_info'
            assert '날짜 범위' in result.content['missingInfoDescription']
            assert len(result.content['followUpQuestions']) > 0

async def test_json_parsing_failure():
    """JSON 파싱 실패 테스트"""
    print("=== 테스트 7: JSON 파싱 실패 ===")
    
    agent = create_schema_analyzer_agent()
    
    # 잘못된 JSON 응답 모킹
    invalid_json_response = "잘못된 JSON 형식의 응답"
    
    with patch('agents.schema_analyzer_agent.schema_retriever') as mock_retriever:
        mock_retriever.vectorstore = True
        mock_retriever.get_relevant_tables_with_threshold.return_value = MOCK_TABLE_DATA
        
        # send_llm_request 메서드 모킹
        with patch.object(agent, 'send_llm_request', return_value=invalid_json_response):
            message = AgentMessage(
                id="test_7",
                type=MessageType.TASK,
                source="test",
                target="schema_analyzer",
                content={
                    "userInput": "테스트 쿼리"
                }
            )
            
            result = await agent.process_message(message)
            print(f"입력: {message.content['userInput']}")
            print(f"결과: {json.dumps(result.content, indent=2, ensure_ascii=False)}")
            print()
            
            # 검증 - fallback response가 반환되어야 함
            assert result.content['analysis_type'] == 'needs_more_info'
            assert 'missingInfoDescription' in result.content
            assert 'followUpQuestions' in result.content

async def test_exception_handling():
    """예외 처리 테스트"""
    print("=== 테스트 8: 예외 처리 ===")
    
    agent = create_schema_analyzer_agent()
    
    with patch('agents.schema_analyzer_agent.schema_retriever') as mock_retriever:
        # 예외 발생하도록 모킹
        mock_retriever.vectorstore = True
        mock_retriever.get_relevant_tables.side_effect = Exception("테스트 예외")
        
        message = AgentMessage(
            id="test_8",
            type=MessageType.TASK,
            source="test",
            target="schema_analyzer",
            content={
                "userInput": "예외 테스트"
            }
        )
        
        result = await agent.process_message(message)
        print(f"입력: {message.content['userInput']}")
        print(f"결과: {json.dumps(result.content, indent=2, ensure_ascii=False)}")
        print()
        
        # 검증 - 적절한 오류 응답이 반환되어야 함
        assert result.content['analysis_type'] == 'needs_more_info'
        assert '오류' in result.content['missingInfoDescription']

async def test_similarity_threshold_filtering():
    """유사도 임계값 필터링 테스트"""
    print("=== 테스트 9: 유사도 임계값 필터링 ===")
    
    # 높은 임계값(0.8)으로 에이전트 생성
    agent = create_schema_analyzer_agent(similarity_threshold=0.8)
    
    with patch('agents.schema_analyzer_agent.schema_retriever') as mock_retriever:
        mock_retriever.vectorstore = True
        # 첫 번째 호출(높은 임계값)에서는 빈 결과
        # 두 번째 호출(낮은 임계값)에서는 fallback 테이블 반환
        mock_retriever.get_relevant_tables_with_threshold.side_effect = [
            [],  # 첫 번째 호출 (높은 임계값)
            [{'table_name': 'potential_table', 'description': '낮은 유사도 테이블'}]  # 두 번째 호출 (낮은 임계값)
        ]
        
        message = AgentMessage(
            id="test_9",
            type=MessageType.TASK,
            source="test",
            target="schema_analyzer",
            content={
                "userInput": "모호한 질문"
            }
        )
        
        result = await agent.process_message(message)
        print(f"임계값: {agent.similarity_threshold}")
        print(f"입력: {message.content['userInput']}")
        print(f"결과: {json.dumps(result.content, indent=2, ensure_ascii=False)}")
        print()
        
        # 검증
        assert result.content['analysis_type'] == 'needs_more_info'
        assert '유사도 임계값' in result.content['missingInfoDescription']
        assert 'potential_table' in str(result.content['followUpQuestions'])

async def test_no_fallback_tables_found():
    """Fallback 테이블도 찾지 못한 경우 테스트"""
    print("=== 테스트 10: Fallback 테이블도 찾지 못한 경우 ===")
    
    agent = create_schema_analyzer_agent(similarity_threshold=0.7)
    
    with patch('agents.schema_analyzer_agent.schema_retriever') as mock_retriever:
        mock_retriever.vectorstore = True
        # 두 번의 호출 모두 빈 결과
        mock_retriever.get_relevant_tables_with_threshold.return_value = []
        
        message = AgentMessage(
            id="test_10",
            type=MessageType.TASK,
            source="test",
            target="schema_analyzer",
            content={
                "userInput": "완전히 관련 없는 질문"
            }
        )
        
        result = await agent.process_message(message)
        print(f"입력: {message.content['userInput']}")
        print(f"결과: {json.dumps(result.content, indent=2, ensure_ascii=False)}")
        print()
        
        # 검증
        assert result.content['analysis_type'] == 'needs_more_info'
        assert '관련된 스키마 정보를 찾을 수 없습니다' in result.content['missingInfoDescription']
        assert any('어떤 종류의 테이블들이' in q for q in result.content['followUpQuestions'])

async def main():
    """모든 테스트 실행"""
    print("SchemaAnalyzerAgent 테스트 시작\n")
    
    try:
        await test_valid_user_input()
        await test_empty_user_input()
        await test_schema_retriever_not_initialized()
        await test_no_relevant_schemas_found()
        await test_schema_context_analysis()
        await test_needs_more_info_analysis()
        await test_json_parsing_failure()
        await test_exception_handling()
        await test_similarity_threshold_filtering()
        await test_no_fallback_tables_found()
        
        print("모든 테스트 완료!")
        
    except Exception as e:
        print(f"테스트 중 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())