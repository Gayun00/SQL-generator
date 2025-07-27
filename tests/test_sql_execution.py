#!/usr/bin/env python3
"""
SQL Execution Test - BigQuery SQL 실행 기능 테스트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.bigquery_client import bq_client
from workflow.nodes import sql_executor
from workflow.state import SQLGeneratorState

def test_bigquery_connection():
    """BigQuery 연결 테스트"""
    print("🔗 BigQuery 연결 테스트 중...")
    
    if bq_client.connect():
        print("✅ BigQuery 연결 성공!")
        return True
    else:
        print("❌ BigQuery 연결 실패!")
        return False

def test_simple_query():
    """간단한 쿼리 실행 테스트"""
    print("\n🔍 간단한 쿼리 실행 테스트 중...")
    
    # 간단한 테스트 쿼리
    test_query = "SELECT 1 as test_column, 'Hello BigQuery' as message, CURRENT_TIMESTAMP() as current_time"
    
    result = bq_client.execute_query(test_query, max_results=10)
    
    if result["success"]:
        print("✅ 쿼리 실행 성공!")
        print(f"   반환된 행 수: {result['returned_rows']}")
        print(f"   결과 데이터: {result['results']}")
        return True
    else:
        print(f"❌ 쿼리 실행 실패: {result['error']}")
        return False

def test_table_query():
    """실제 테이블 쿼리 테스트"""
    print("\n📊 실제 테이블 쿼리 테스트 중...")
    
    # bq_client.schema_info에서 첫 번째 테이블 선택
    if not bq_client.schema_info:
        print("⚠️ 스키마 정보가 없습니다. 먼저 스키마를 초기화하세요.")
        return False
    
    # 첫 번째 테이블 선택
    table_name = list(bq_client.schema_info.keys())[0]
    print(f"   테스트 테이블: {table_name}")
    
    # LIMIT을 사용한 안전한 쿼리
    test_query = f"SELECT * FROM `{table_name}` LIMIT 5"
    
    result = bq_client.execute_query(test_query, max_results=10)
    
    if result["success"]:
        print("✅ 테이블 쿼리 실행 성공!")
        print(f"   반환된 행 수: {result['returned_rows']}")
        print(f"   총 행 수: {result['total_rows']}")
        print(f"   처리된 바이트: {result['bytes_processed']:,}")
        
        # 첫 번째 결과 출력
        if result['results']:
            print(f"   첫 번째 행: {result['results'][0]}")
        return True
    else:
        print(f"❌ 테이블 쿼리 실행 실패: {result['error']}")
        print(f"   오류 유형: {result.get('error_type', 'unknown')}")
        print(f"   제안사항: {result.get('suggestion', 'N/A')}")
        return False

def test_invalid_query():
    """잘못된 쿼리 테스트 (에러 처리 확인)"""
    print("\n❌ 잘못된 쿼리 테스트 중...")
    
    # 의도적으로 잘못된 쿼리
    invalid_query = "SELECT * FROM non_existent_table"
    
    result = bq_client.execute_query(invalid_query)
    
    if not result["success"]:
        print("✅ 에러 처리 정상 작동!")
        print(f"   오류: {result['error']}")
        print(f"   오류 유형: {result.get('error_type', 'unknown')}")
        print(f"   제안사항: {result.get('suggestion', 'N/A')}")
        return True
    else:
        print("❌ 에러 처리 실패 - 잘못된 쿼리가 성공했습니다!")
        return False

async def test_sql_executor_node():
    """SQL_Executor 노드 테스트"""
    print("\n⚡ SQL_Executor 노드 테스트 중...")
    
    # 테스트 상태 생성
    test_state = {
        "userInput": "테스트 쿼리",
        "isValid": True,
        "sqlQuery": "SELECT 1 as number, 'test' as text, CURRENT_TIMESTAMP() as timestamp",
        "queryResults": None,
        "executionStatus": None
    }
    
    # SQL_Executor 노드 실행
    result_state = await sql_executor(test_state)
    
    if result_state.get("executionStatus") == "success":
        print("✅ SQL_Executor 노드 테스트 성공!")
        print(f"   실행 상태: {result_state['executionStatus']}")
        print(f"   반환된 행 수: {result_state['queryResults']['returned_rows']}")
        return True
    else:
        print(f"❌ SQL_Executor 노드 테스트 실패!")
        print(f"   실행 상태: {result_state.get('executionStatus', 'unknown')}")
        if result_state.get('queryResults'):
            print(f"   오류: {result_state['queryResults'].get('error', 'N/A')}")
        return False

async def main():
    """전체 테스트 실행"""
    print("🚀 SQL 실행 기능 테스트 시작!")
    print("=" * 60)
    
    tests = [
        ("BigQuery 연결", test_bigquery_connection),
        ("간단한 쿼리 실행", test_simple_query),
        ("실제 테이블 쿼리", test_table_query),
        ("잘못된 쿼리 처리", test_invalid_query),
        ("SQL_Executor 노드", test_sql_executor_node)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n📋 테스트: {test_name}")
        print("-" * 40)
        
        try:
            import asyncio
            import inspect
            
            # 비동기 함수인지 확인
            if asyncio.iscoroutinefunction(test_func):
                result = await test_func()
            else:
                result = test_func()
                
            if result:
                passed += 1
                print(f"✅ {test_name} 통과")
            else:
                print(f"❌ {test_name} 실패")
                
        except Exception as e:
            print(f"💥 {test_name} 오류: {str(e)}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 60)
    print(f"🎯 테스트 결과: {passed}/{total} 통과")
    
    if passed == total:
        print("🎉 모든 테스트 통과!")
    else:
        print(f"⚠️ {total - passed}개 테스트 실패")
    
    return passed == total

if __name__ == "__main__":
    import asyncio
    
    # 스키마 정보가 필요한 테스트를 위해 초기화
    print("🔍 스키마 정보 초기화 중...")
    if bq_client.connect():
        bq_client.initialize_schema()
    
    # 비동기 테스트 실행
    asyncio.run(main())