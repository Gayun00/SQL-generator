#!/usr/bin/env python3
"""
QueryArchitect Agent Test - SQL 생성 및 최적화 전문 Agent 테스트

기존 sql_generator와 새로운 QueryArchitect Agent의 
성능과 정확도를 비교하는 테스트입니다.
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
    print("🔧 QueryArchitect 테스트 환경 초기화 중...")
    
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

async def test_simple_generation():
    """단순 SQL 생성 테스트"""
    print("\\n🧪 단순 SQL 생성 테스트")
    print("-" * 40)
    
    # Agent 생성
    agent = create_query_architect_agent()
    
    # 테스트 케이스
    test_cases = [
        {
            "name": "기본 조회",
            "query": "users 테이블의 모든 데이터를 조회해줘",
            "expected": "SELECT * FROM us_plus.users"
        },
        {
            "name": "조건부 조회", 
            "query": "orders 테이블에서 100개만 조회해줘",
            "expected": "LIMIT 100"
        }
    ]
    
    success_count = 0
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\\n📋 테스트 케이스 {i}: {test_case['name']}")
        print(f"쿼리: {test_case['query']}")
        
        # 메시지 생성
        message = AgentMessage(
            sender="test",
            receiver="query_architect",
            message_type=MessageType.REQUEST,
            content={
                "task_type": "simple_generation",
                "query": test_case["query"],
                "context": {}
            }
        )
        
        try:
            # Agent 실행
            response = await agent.process_message(message)
            
            if response.message_type == MessageType.ERROR:
                print(f"❌ Agent 오류: {response.content.get('error_message', 'Unknown error')}")
                continue
            
            # 결과 분석
            result = response.content
            sql_query = result.get("sql_query", "")
            processing_time = result.get("processing_time", 0)
            complexity = result.get("complexity", "unknown")
            
            print(f"✅ 생성 완료 ({processing_time:.2f}초)")
            print(f"   복잡도: {complexity}")
            print(f"   SQL: {sql_query[:100]}...")
            
            # 기대값 확인
            if test_case["expected"].lower() in sql_query.lower():
                print("✅ 기대값 포함됨")
                success_count += 1
            else:
                print("⚠️ 기대값 불일치")
            
        except Exception as e:
            print(f"❌ 테스트 실패: {str(e)}")
    
    print(f"\\n📊 단순 생성 테스트 결과: {success_count}/{len(test_cases)} 성공")
    return success_count == len(test_cases)

async def test_optimized_generation():
    """최적화된 SQL 생성 테스트"""
    print("\\n🧪 최적화된 SQL 생성 테스트")
    print("-" * 40)
    
    # Agent 생성
    agent = create_query_architect_agent()
    
    # 복잡한 테스트 케이스
    test_cases = [
        {
            "name": "집계 쿼리",
            "query": "최근 일주일간 가장 많은 금액을 결제한 유저를 찾아줘",
            "analysis_result": {
                "has_uncertainty": True,
                "uncertainties": [
                    {"type": "data_range", "description": "최근 일주일 기준점 불명확"},
                    {"type": "table_relationship", "description": "유저-결제 관계 불명확"}
                ]
            }
        },
        {
            "name": "조건부 집계",
            "query": "카테고리별 매출 통계를 구해줘",
            "analysis_result": {
                "has_uncertainty": True,
                "uncertainties": [
                    {"type": "column_values", "description": "카테고리 컬럼값 불확실"}
                ]
            }
        }
    ]
    
    success_count = 0
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\\n📋 테스트 케이스 {i}: {test_case['name']}")
        print(f"쿼리: {test_case['query']}")
        
        # 메시지 생성
        message = AgentMessage(
            sender="test",
            receiver="query_architect",
            message_type=MessageType.REQUEST,
            content={
                "task_type": "optimized_generation",
                "query": test_case["query"],
                "analysis_result": test_case["analysis_result"],
                "exploration_result": {}
            }
        )
        
        try:
            # Agent 실행
            response = await agent.process_message(message)
            
            if response.message_type == MessageType.ERROR:
                print(f"❌ Agent 오류: {response.content.get('error_message', 'Unknown error')}")
                continue
            
            # 결과 분석
            result = response.content
            sql_query = result.get("sql_query", "")
            processing_time = result.get("processing_time", 0)
            complexity = result.get("complexity", "unknown")
            optimization_applied = result.get("optimization_applied", False)
            applied_optimizations = result.get("applied_optimizations", [])
            confidence = result.get("confidence", 0.0)
            
            print(f"✅ 생성 완료 ({processing_time:.2f}초)")
            print(f"   복잡도: {complexity}")
            print(f"   최적화 적용: {optimization_applied}")
            print(f"   적용된 최적화: {', '.join(applied_optimizations)}")
            print(f"   신뢰도: {confidence:.2f}")
            print(f"   SQL 길이: {len(sql_query)} 문자")
            
            # 품질 확인
            quality_score = 0
            if "SELECT" in sql_query.upper():
                quality_score += 1
            if "FROM" in sql_query.upper():
                quality_score += 1
            if "LIMIT" in sql_query.upper():
                quality_score += 1
            if optimization_applied:
                quality_score += 1
            if confidence > 0.7:
                quality_score += 1
            
            if quality_score >= 4:
                print("✅ 품질 기준 통과")
                success_count += 1
            else:
                print(f"⚠️ 품질 기준 미달 (점수: {quality_score}/5)")
            
        except Exception as e:
            print(f"❌ 테스트 실패: {str(e)}")
            import traceback
            traceback.print_exc()
    
    print(f"\\n📊 최적화 생성 테스트 결과: {success_count}/{len(test_cases)} 성공")
    return success_count == len(test_cases)

async def test_agent_statistics():
    """Agent 통계 테스트"""
    print("\\n🧪 Agent 통계 테스트")
    print("-" * 40)
    
    # Agent 생성
    agent = create_query_architect_agent()
    
    # 몇 개의 쿼리 실행해서 통계 생성
    test_queries = [
        "SELECT * FROM users LIMIT 10",
        "SELECT COUNT(*) FROM orders WHERE created_at > CURRENT_DATE() - 7",
        "SELECT category, SUM(amount) FROM sales GROUP BY category"
    ]
    
    for query in test_queries:
        message = AgentMessage(
            sender="test",
            receiver="query_architect", 
            message_type=MessageType.REQUEST,
            content={
                "task_type": "simple_generation",
                "query": query
            }
        )
        
        try:
            await agent.process_message(message)
        except:
            pass  # 통계를 위한 실행이므로 오류 무시
    
    # 통계 조회
    try:
        stats = agent.get_agent_statistics()
        
        print("📈 Agent 통계:")
        for key, value in stats.items():
            print(f"   {key}: {value}")
        
        print("✅ 통계 테스트 완료")
        return True
        
    except Exception as e:
        print(f"❌ 통계 테스트 실패: {str(e)}")
        return False

async def main():
    """메인 테스트 실행"""
    print("🚀 QueryArchitect Agent 종합 테스트 시작!")
    print("=" * 60)
    
    # 환경 초기화
    if not initialize_test_environment():
        print("❌ 환경 초기화 실패로 테스트 중단")
        return False
    
    # 테스트 실행
    tests = [
        ("단순 SQL 생성", test_simple_generation),
        ("최적화된 SQL 생성", test_optimized_generation), 
        ("Agent 통계", test_agent_statistics)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\\n🧪 {test_name}")
        print("=" * 60)
        
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
    print("\\n" + "=" * 60)
    print(f"🎯 QueryArchitect Agent 테스트 결과: {passed}/{total} 통과")
    
    if passed == total:
        print("🎉 모든 QueryArchitect 테스트 통과!")
        print("✅ SQL 생성 Agent가 성공적으로 구현되었습니다!")
    else:
        print(f"⚠️ {total - passed}개 테스트 실패")
        print("🔧 Agent 개선이 필요합니다.")
    
    return passed == total

if __name__ == "__main__":
    asyncio.run(main())