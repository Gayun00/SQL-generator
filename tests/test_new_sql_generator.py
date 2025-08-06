"""
SQLGenerator (newAgents version) 테스트 케이스
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import json
from unittest.mock import Mock, patch
from newAgents.sql_generator_agent import SQLGenerator


async def test_successful_sql_generation():
    """성공적인 SQL 생성 테스트"""
    print("=== 테스트 1: 성공적인 SQL 생성 ===")
    
    generator = SQLGenerator()
    
    test_data = {
        "session_id": "test_session_1",
        "content": {
            "user_query": "사용자 목록을 보여줘",
            "schema_info": [
                {
                    "table_name": "users",
                    "columns": [
                        {"name": "user_id", "type": "STRING"},
                        {"name": "email", "type": "STRING"},
                        {"name": "created_at", "type": "TIMESTAMP"}
                    ]
                }
            ]
        }
    }
    
    result = await generator.process(test_data)
    
    print(f"입력: {test_data['content']['user_query']}")
    print(f"결과: {json.dumps(result, indent=2, ensure_ascii=False)}")
    print()
    
    # 검증
    assert result["status"] == "success"
    assert "generated_sql" in result
    assert "SELECT * FROM users;" in result["generated_sql"]


async def test_invalid_input_no_schema():
    """스키마 정보가 없는 경우 테스트"""
    print("=== 테스트 2: 스키마 정보 없음 ===")
    
    generator = SQLGenerator()
    
    test_data = {
        "session_id": "test_session_2", 
        "content": {
            "user_query": "사용자 정보를 보여줘",
            "schema_info": []
        }
    }
    
    result = await generator.process(test_data)
    
    print(f"입력: {test_data['content']['user_query']}")
    print(f"결과: {json.dumps(result, indent=2, ensure_ascii=False)}")
    print()
    
    # 검증 - MockChatOpenAI는 빈 스키마에도 기본 SQL을 생성함
    assert result["status"] == "success"
    assert "generated_sql" in result


async def test_invalid_input_no_query():
    """사용자 쿼리가 없는 경우 테스트"""
    print("=== 테스트 3: 사용자 쿼리 없음 ===")
    
    generator = SQLGenerator()
    
    test_data = {
        "session_id": "test_session_3",
        "content": {
            "user_query": "",
            "schema_info": [{"table_name": "users"}]
        }
    }
    
    result = await generator.process(test_data)
    
    print(f"입력: '{test_data['content']['user_query']}'")
    print(f"결과: {json.dumps(result, indent=2, ensure_ascii=False)}")
    print()
    
    # 검증 - MockChatOpenAI는 빈 쿼리에도 기본 SQL을 생성함
    assert result["status"] == "success"
    assert "generated_sql" in result


async def test_llm_exception_handling():
    """LLM 호출 중 예외 발생 테스트"""
    print("=== 테스트 4: LLM 예외 처리 ===")
    
    generator = SQLGenerator()
    
    test_data = {
        "session_id": "test_session_4",
        "content": {
            "user_query": "실패",  # MockChatOpenAI에서 None을 반환하는 키워드
            "schema_info": [{"table_name": "users"}]
        }
    }
    
    result = await generator.process(test_data)
    
    print(f"입력: {test_data['content']['user_query']}")
    print(f"결과: {json.dumps(result, indent=2, ensure_ascii=False)}")
    print()
    
    # 검증 - MockChatOpenAI가 None을 반환하면 SQL 생성 실패로 처리됨
    assert result["status"] == "error"
    assert "error_message" in result


async def test_complex_schema_processing():
    """복잡한 스키마 정보 처리 테스트"""
    print("=== 테스트 5: 복잡한 스키마 처리 ===")
    
    generator = SQLGenerator()
    
    test_data = {
        "session_id": "test_session_5",
        "content": {
            "user_query": "매출을 보여줘",  # MockChatOpenAI에서 orders 테이블 쿼리를 생성하는 키워드
            "schema_info": [
                {
                    "table_name": "users",
                    "columns": [
                        {"name": "user_id", "type": "STRING"},
                        {"name": "email", "type": "STRING"}
                    ]
                },
                {
                    "table_name": "orders", 
                    "columns": [
                        {"name": "order_id", "type": "STRING"},
                        {"name": "user_id", "type": "STRING"},
                        {"name": "amount", "type": "FLOAT64"}
                    ]
                }
            ]
        }
    }
    
    result = await generator.process(test_data)
    
    print(f"입력: {test_data['content']['user_query']}")
    print(f"결과: {json.dumps(result, indent=2, ensure_ascii=False)}")
    print()
    
    # 검증
    assert result["status"] == "success"
    assert "generated_sql" in result
    assert "SELECT SUM(amount) FROM orders;" in result["generated_sql"]


async def main():
    """모든 테스트 실행"""
    print("SQLGenerator (newAgents) 테스트 시작\n")
    
    try:
        await test_successful_sql_generation()
        await test_invalid_input_no_schema()
        await test_invalid_input_no_query()
        await test_llm_exception_handling()
        await test_complex_schema_processing()
        
        print("모든 테스트 완료!")
        
    except Exception as e:
        print(f"테스트 중 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())