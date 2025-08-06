"""
sub_agents (data_explorer, sql_executor) 테스트 케이스
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import json
from newAgents.sub_agents import data_explorer, sql_executor


async def test_data_explorer_success():
    """Data Explorer 성공 케이스 테스트"""
    print("=== 테스트 1: Data Explorer 성공 ===")
    
    state = {
        "session_id": "test_session_1",
        "sql_generator_result": {
            "info_request": {
                "query": "SELECT DISTINCT status FROM users",
                "type": "db_info"
            }
        }
    }
    
    result = await data_explorer(state)
    
    print(f"입력 상태: {json.dumps(state, indent=2, ensure_ascii=False)}")
    print(f"결과: {json.dumps(result, indent=2, ensure_ascii=False)}")
    print()
    
    # 검증
    assert result["status"] == "success"
    assert "data_sample" in result
    assert "confidence" in result
    assert isinstance(result["data_sample"], list)
    assert len(result["data_sample"]) > 0


async def test_data_explorer_no_info_request():
    """Data Explorer - info_request 없는 경우"""
    print("=== 테스트 2: Data Explorer - info_request 없음 ===")
    
    state = {
        "session_id": "test_session_2",
        "sql_generator_result": {}
    }
    
    result = await data_explorer(state)
    
    print(f"입력 상태: {json.dumps(state, indent=2, ensure_ascii=False)}")
    print(f"결과: {json.dumps(result, indent=2, ensure_ascii=False)}")
    print()
    
    # 검증 - 기본적으로 성공을 반환하지만 info_request가 없어도 동작
    assert result["status"] == "success"
    assert "data_sample" in result


async def test_data_explorer_empty_state():
    """Data Explorer - 빈 상태 테스트"""
    print("=== 테스트 3: Data Explorer - 빈 상태 ===")
    
    state = {}
    
    result = await data_explorer(state)
    
    print(f"입력 상태: {json.dumps(state, indent=2, ensure_ascii=False)}")
    print(f"결과: {json.dumps(result, indent=2, ensure_ascii=False)}")
    print()
    
    # 검증
    assert result["status"] == "success"
    assert "data_sample" in result
    assert "confidence" in result


async def test_sql_executor_success():
    """SQL Executor 성공 케이스 테스트"""
    print("=== 테스트 4: SQL Executor 성공 ===")
    
    state = {
        "session_id": "test_session_4",
        "sql_generator_result": {
            "generated_sql": "SELECT user_id, email FROM users WHERE created_at >= '2025-01-01'",
            "status": "success"
        }
    }
    
    result = await sql_executor(state)
    
    print(f"입력 상태: {json.dumps(state, indent=2, ensure_ascii=False)}")
    print(f"결과: {json.dumps(result, indent=2, ensure_ascii=False)}")
    print()
    
    # 검증
    assert result["status"] == "success"
    assert "execution_result" in result
    assert isinstance(result["execution_result"], list)
    assert len(result["execution_result"]) > 0


async def test_sql_executor_no_sql():
    """SQL Executor - 생성된 SQL이 없는 경우"""
    print("=== 테스트 5: SQL Executor - SQL 없음 ===")
    
    state = {
        "session_id": "test_session_5",
        "sql_generator_result": {}
    }
    
    result = await sql_executor(state)
    
    print(f"입력 상태: {json.dumps(state, indent=2, ensure_ascii=False)}")
    print(f"결과: {json.dumps(result, indent=2, ensure_ascii=False)}")
    print()
    
    # 검증 - 현재 구현은 항상 성공을 반환 (mocked)
    assert result["status"] == "success"
    assert "execution_result" in result


async def test_sql_executor_empty_state():
    """SQL Executor - 빈 상태 테스트"""  
    print("=== 테스트 6: SQL Executor - 빈 상태 ===")
    
    state = {}
    
    result = await sql_executor(state)
    
    print(f"입력 상태: {json.dumps(state, indent=2, ensure_ascii=False)}")
    print(f"결과: {json.dumps(result, indent=2, ensure_ascii=False)}")
    print()
    
    # 검증
    assert result["status"] == "success"
    assert "execution_result" in result


async def test_workflow_integration():
    """워크플로우 통합 테스트 - data_explorer → sql_executor"""
    print("=== 테스트 7: 워크플로우 통합 테스트 ===")
    
    # 1단계: Data Explorer
    initial_state = {
        "session_id": "test_integration",
        "sql_generator_result": {
            "info_request": {
                "query": "SELECT COUNT(*) FROM orders WHERE status = 'completed'",
                "type": "db_info"
            }
        }
    }
    
    explorer_result = await data_explorer(initial_state)
    
    # 2단계: SQL Executor (data_explorer 결과를 상태에 추가)
    executor_state = initial_state.copy()
    executor_state["data_explorer_result"] = explorer_result
    executor_state["sql_generator_result"]["generated_sql"] = "SELECT * FROM orders WHERE status = 'completed'"
    
    executor_result = await sql_executor(executor_state)
    
    print(f"Data Explorer 결과: {json.dumps(explorer_result, indent=2, ensure_ascii=False)}")
    print(f"SQL Executor 결과: {json.dumps(executor_result, indent=2, ensure_ascii=False)}")
    print()
    
    # 검증
    assert explorer_result["status"] == "success"
    assert executor_result["status"] == "success"
    assert "data_sample" in explorer_result
    assert "execution_result" in executor_result


async def main():
    """모든 테스트 실행"""
    print("Sub Agents (data_explorer, sql_executor) 테스트 시작\n")
    
    try:
        await test_data_explorer_success()
        await test_data_explorer_no_info_request()
        await test_data_explorer_empty_state()
        await test_sql_executor_success()
        await test_sql_executor_no_sql()
        await test_sql_executor_empty_state()
        await test_workflow_integration()
        
        print("모든 테스트 완료!")
        
    except Exception as e:
        print(f"테스트 중 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())