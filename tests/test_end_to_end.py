#!/usr/bin/env python3
"""
End-to-End Integration Test - 전체 워크플로우 통합 테스트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from workflow.a2a_workflow import create_a2a_workflow
from db.bigquery_client import bq_client
from rag.schema_embedder import schema_embedder
from rag.schema_retriever import schema_retriever

def initialize_test_environment():
    """테스트 환경 초기화"""
    print("🔍 엔드투엔드 테스트 환경 초기화 중...")
    
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

async def test_simple_query_flow():
    """간단한 쿼리의 전체 플로우 테스트"""
    print("\n🔍 간단한 쿼리 전체 플로우 테스트 중...")
    
    # A2A 워크플로우 생성
    workflow_manager = create_a2a_workflow()
    
    # 간단한 테스트 쿼리
    test_input = "users 테이블에서 모든 사용자 정보 조회"
    
    try:
        print(f"📝 입력: {test_input}")
        result = await workflow_manager.process_query(test_input)
        
        # 결과 검증
        success_checks = []
        
        # 1. 유효성 검증 통과
        if result.get("isValid"):
            success_checks.append("✅ 입력 유효성 검증 통과")
        else:
            success_checks.append("❌ 입력 유효성 검증 실패")
        
        # 2. SQL 쿼리 생성
        if result.get("sqlQuery"):
            success_checks.append("✅ SQL 쿼리 생성 완료")
        else:
            success_checks.append("❌ SQL 쿼리 생성 실패")
        
        # 3. SQL 실행
        if result.get("queryResults"):
            success_checks.append("✅ SQL 실행 완료")
        else:
            success_checks.append("❌ SQL 실행 실패")
        
        # 4. 설명 생성
        if result.get("explanation"):
            success_checks.append("✅ 설명 생성 완료")
        else:
            success_checks.append("❌ 설명 생성 실패")
        
        # 5. 최종 출력
        if result.get("finalOutput"):
            success_checks.append("✅ 최종 출력 생성 완료")
        else:
            success_checks.append("❌ 최종 출력 생성 실패")
        
        print("📊 플로우 체크 결과:")
        for check in success_checks:
            print(f"   {check}")
        
        success_count = len([c for c in success_checks if c.startswith("✅")])
        total_count = len(success_checks)
        
        if success_count == total_count:
            print("✅ 간단한 쿼리 전체 플로우 성공!")
            return True
        else:
            print(f"⚠️ 일부 단계 실패: {success_count}/{total_count}")
            return False
            
    except Exception as e:
        print(f"❌ 플로우 실행 중 오류: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

async def test_uncertain_query_flow():
    """불확실성이 있는 쿼리의 전체 플로우 테스트"""
    print("\n🔍 불확실성 있는 쿼리 전체 플로우 테스트 중...")
    
    # A2A 워크플로우 생성
    workflow_manager = create_a2a_workflow()
    
    # 불확실성이 있는 테스트 쿼리
    test_input = "상태가 '활성'인 사용자들의 주문 내역을 보여줘"
    
    try:
        print(f"📝 입력: {test_input}")
        result = await workflow_manager.process_query(test_input)
        
        # 결과 검증
        success_checks = []
        
        # 1. 유효성 검증 통과
        if result.get("isValid"):
            success_checks.append("✅ 입력 유효성 검증 통과")
        else:
            success_checks.append("❌ 입력 유효성 검증 실패")
        
        # 2. 불확실성 분석
        if result.get("uncertaintyAnalysis"):
            success_checks.append("✅ 불확실성 분석 완료")
            
            # 불확실성이 탐지되었는지 확인
            if result.get("hasUncertainty"):
                success_checks.append("✅ 불확실성 탐지됨")
            else:
                success_checks.append("⚠️ 불확실성 탐지되지 않음")
        else:
            success_checks.append("❌ 불확실성 분석 실패")
        
        # 3. 탐색 결과
        if result.get("explorationResults"):
            success_checks.append("✅ 추가 쿼리 실행 완료")
        else:
            success_checks.append("❌ 추가 쿼리 실행 실패")
        
        # 4. SQL 생성 (탐색 결과 활용)
        if result.get("sqlQuery"):
            success_checks.append("✅ SQL 쿼리 생성 완료")
        else:
            success_checks.append("❌ SQL 쿼리 생성 실패")
        
        # 5. SQL 실행
        if result.get("queryResults"):
            success_checks.append("✅ SQL 실행 완료")
        else:
            success_checks.append("❌ SQL 실행 실패")
        
        print("📊 불확실성 처리 플로우 체크 결과:")
        for check in success_checks:
            print(f"   {check}")
        
        success_count = len([c for c in success_checks if c.startswith("✅")])
        total_count = len(success_checks)
        
        if success_count >= total_count - 1:  # 1개 정도 실패는 허용
            print("✅ 불확실성 있는 쿼리 플로우 성공!")
            return True
        else:
            print(f"⚠️ 너무 많은 단계 실패: {success_count}/{total_count}")
            return False
            
    except Exception as e:
        print(f"❌ 플로우 실행 중 오류: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

async def test_workflow_node_connections():
    """워크플로우 노드 연결 테스트"""
    print("\n🔍 워크플로우 노드 연결 테스트 중...")
    
    try:
        # A2A 워크플로우 생성이 성공하는지 확인
        workflow_manager = create_a2a_workflow()
        
        if workflow_manager:
            print("✅ A2A 워크플로우 생성 성공")
            
            # 간단한 테스트로 구조 확인
            try:
                # A2A 워크플로우 구조 확인
                agents = workflow_manager.get_available_agents()
                if agents:
                    print(f"✅ 사용 가능한 Agent 수: {len(agents)}")
                    print("✅ A2A 워크플로우 구조 유효성 확인 완료")
                    return True
                else:
                    print("❌ 사용 가능한 Agent 없음")
                    return False
            except Exception as e:
                print(f"❌ A2A 워크플로우 구조 오류: {str(e)}")
                return False
        else:
            print("❌ A2A 워크플로우 생성 실패")
            return False
            
    except Exception as e:
        print(f"❌ 워크플로우 노드 연결 테스트 실패: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """전체 엔드투엔드 테스트 실행"""
    print("🚀 엔드투엔드 통합 테스트 시작!")
    print("=" * 60)
    
    # 테스트 환경 초기화
    if not initialize_test_environment():
        print("❌ 테스트 환경 초기화 실패")
        return False
    
    tests = [
        ("A2A 워크플로우 Agent 연결", test_workflow_node_connections),
        ("간단한 쿼리 전체 플로우", test_simple_query_flow),
        ("불확실성 있는 쿼리 플로우", test_uncertain_query_flow)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n📋 테스트: {test_name}")
        print("-" * 40)
        
        try:
            result = await test_func()
            
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
    print(f"🎯 엔드투엔드 테스트 결과: {passed}/{total} 통과")
    
    if passed == total:
        print("🎉 모든 엔드투엔드 테스트 통과!")
        print("✅ 전체 시스템이 정상적으로 통합되었습니다!")
    else:
        print(f"⚠️ {total - passed}개 테스트 실패")
        print("🔧 시스템 통합에 문제가 있을 수 있습니다.")
    
    return passed == total

if __name__ == "__main__":
    asyncio.run(main())