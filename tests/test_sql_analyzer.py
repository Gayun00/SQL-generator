#!/usr/bin/env python3
"""
DEPRECATED: SQL Analyzer Test - Langgraph 노드 테스트 (A2A 전환으로 비활성화됨)

이 테스트는 Langgraph 기반 workflow.nodes 모듈의 sql_analyzer 노드를 테스트했지만,
A2A 아키텍처 전환으로 더 이상 사용되지 않습니다.

대신 다음 테스트를 사용하세요:
- tests/test_complete_a2a_system.py: 완전한 A2A 시스템 테스트
- test_dynamic_flow.py: 동적 플로우 테스트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
# DEPRECATED: workflow.nodes 모듈은 A2A 전환으로 제거됨
# from workflow.nodes import sql_analyzer
# from workflow.state import SQLGeneratorState
from db.bigquery_client import bq_client
from rag.schema_embedder import schema_embedder
from rag.schema_retriever import schema_retriever

def initialize_test_environment():
    """테스트 환경 초기화"""
    print("🔍 테스트 환경 초기화 중...")
    
    # BigQuery 클라이언트와 스키마 초기화
    schema_info = schema_embedder.initialize_with_cache(bq_client)
    
    if not schema_info:
        print("❌ 스키마 정보 초기화 실패")
        return False
    
    print(f"✅ 스키마 정보 초기화 완료: {len(schema_info)}개 테이블")
    
    # 스키마 검색기 초기화
    if not schema_retriever.initialize():
        print("❌ 스키마 검색기 초기화 실패")
        return False
    
    print("✅ 스키마 검색기 초기화 완료")
    return True

async def test_column_values_uncertainty():
    """DEPRECATED: 컬럼 값 불확실성 테스트 (A2A 전환으로 비활성화됨)"""
    print("\n⚠️ Langgraph 기반 SQL Analyzer 테스트는 더 이상 지원되지 않습니다.")
    print("   A2A 시스템에서는 SchemaIntelligence Agent가 불확실성 분석을 담당합니다.")
    print("   tests/test_complete_a2a_system.py를 사용하여 A2A 불확실성 분석을 테스트하세요.")
    return True

async def test_table_relationship_uncertainty():
    """DEPRECATED: 테이블 관계 불확실성 테스트 (A2A 전환으로 비활성화됨)"""
    print("\n⚠️ Langgraph 기반 테이블 관계 불확실성 테스트는 더 이상 지원되지 않습니다.")
    print("   A2A 시스템에서는 SchemaIntelligence Agent가 관계 분석을 담당합니다.")
    print("   tests/test_complete_a2a_system.py를 사용하여 A2A 관계 분석을 테스트하세요.")
    return True

async def test_data_range_uncertainty():
    """DEPRECATED: 데이터 범위 불확실성 테스트 (A2A 전환으로 비활성화됨)"""
    print("\n⚠️ Langgraph 기반 데이터 범위 불확실성 테스트는 더 이상 지원되지 않습니다.")
    print("   A2A 시스템에서는 DataInvestigator Agent가 범위 분석을 담당합니다.")
    print("   test_dynamic_flow.py를 사용하여 A2A 동적 데이터 탐색을 테스트하세요.")
    return True

async def test_no_uncertainty():
    """DEPRECATED: 불확실성 탐지 정확성 테스트 (A2A 전환으로 비활성화됨)"""
    print("\n⚠️ Langgraph 기반 불확실성 탐지 정확성 테스트는 더 이상 지원되지 않습니다.")
    print("   A2A 시스템에서는 MasterOrchestrator가 동적으로 플로우를 조정합니다.")
    print("   test_dynamic_flow.py를 사용하여 A2A 동적 플로우를 테스트하세요.")
    return True

async def test_json_parsing():
    """DEPRECATED: JSON 파싱 및 오류 처리 테스트 (A2A 전환으로 비활성화됨)"""
    print("\n⚠️ Langgraph 기반 JSON 파싱 테스트는 더 이상 지원되지 않습니다.")
    print("   A2A 시스템에서는 각 Agent가 개별적으로 오류를 처리합니다.")
    print("   test_dynamic_flow.py의 error_handling_flow를 사용하여 A2A 오류 처리를 테스트하세요.")
    return True

async def main():
    """DEPRECATED: 전체 테스트 실행 (A2A 전환으로 비활성화됨)"""
    print("🚀 DEPRECATED: SQL Analyzer 테스트")
    print("=" * 60)
    print("⚠️ 이 테스트는 Langgraph 기반 워크플로우용으로 더 이상 사용되지 않습니다.")
    print("\n🔄 A2A 시스템으로 전환되었습니다. 다음 테스트를 사용하세요:")
    print("   • tests/test_complete_a2a_system.py - 완전한 A2A 시스템 테스트")
    print("   • test_dynamic_flow.py - 동적 플로우 및 불확실성 분석 테스트")
    print("\n✅ 호환성을 위해 모든 테스트를 통과로 처리합니다.")
    
    tests = [
        ("컬럼 값 불확실성 탐지 (DEPRECATED)", test_column_values_uncertainty),
        ("테이블 관계 불확실성 탐지 (DEPRECATED)", test_table_relationship_uncertainty),
        ("데이터 범위 불확실성 탐지 (DEPRECATED)", test_data_range_uncertainty),
        ("불확실성 탐지 정확성 (DEPRECATED)", test_no_uncertainty),
        ("JSON 파싱 및 오류 처리 (DEPRECATED)", test_json_parsing)
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
    print(f"🎯 테스트 결과: {passed}/{total} 통과")
    print("\n🔄 A2A 시스템 테스트로 마이그레이션하세요!")
    
    return passed == total

if __name__ == "__main__":
    asyncio.run(main())