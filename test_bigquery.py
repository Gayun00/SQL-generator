#!/usr/bin/env python3
"""
BigQuery 연결 및 스키마 조회 테스트 스크립트
"""

import asyncio
from bigquery_client import bq_client
from config import BIGQUERY_CONFIG

async def test_bigquery_connection():
    """BigQuery 연결 테스트"""
    print("🚀 BigQuery 연결 테스트 시작")
    print("=" * 60)
    
    # 설정 정보 출력
    print("📋 현재 설정:")
    print(f"   Default Dataset: {BIGQUERY_CONFIG['default_dataset']}")
    print(f"   Target Tables: {BIGQUERY_CONFIG['target_tables']}")
    print(f"   Keyfile Path: {BIGQUERY_CONFIG['keyfile_path']}")
    print("   Project ID: keyfile.json에서 자동 추출")
    print()
    
    # BigQuery 연결 시도
    print("🔗 BigQuery 연결 시도 중...")
    success = bq_client.connect()
    
    if not success:
        print("❌ BigQuery 연결에 실패했습니다.")
        print("\n💡 해결 방법:")
        print("1. keyfile.json이 프로젝트 루트에 존재하는지 확인")
        print("2. keyfile.json이 올바른 서비스 계정 키 파일인지 확인")
        print("3. 서비스 계정에 BigQuery 권한이 있는지 확인")
        print("4. 해당 프로젝트에 BigQuery API가 활성화되어 있는지 확인")
        return False
    
    print("✅ BigQuery 연결 성공!")
    print()
    
    # 스키마 정보 초기화
    print("🔍 스키마 정보 수집 시작...")
    schema_info = bq_client.initialize_schema()
    
    if not schema_info:
        print("❌ 스키마 정보 수집에 실패했습니다.")
        print("\n💡 확인 사항:")
        print("1. BIGQUERY_DEFAULT_DATASET이 존재하는지 확인")
        print("2. BIGQUERY_TARGET_TABLES에 지정된 테이블들이 존재하는지 확인")
        print("3. 서비스 계정에 해당 데이터셋/테이블 읽기 권한이 있는지 확인")
        return False
    
    print(f"✅ 스키마 정보 수집 완료! ({len(schema_info)}개 테이블)")
    print()
    
    # 스키마 정보 상세 출력
    print("📊 수집된 스키마 정보:")
    print("=" * 60)
    
    for table_name, schema in schema_info.items():
        print(f"\n🏷️  테이블: {table_name}")
        if schema.get("description"):
            print(f"   📝 설명: {schema['description']}")
        
        print(f"   📋 컬럼 수: {len(schema.get('columns', []))}")
        for i, col in enumerate(schema.get("columns", [])[:5]):  # 처음 5개만 표시
            col_desc = f" - {col.get('description', '')}" if col.get('description') else ""
            print(f"      {i+1}. {col['name']} ({col['type']}, {col['mode']}){col_desc}")
        
        if len(schema.get("columns", [])) > 5:
            print(f"      ... 및 {len(schema.get('columns', [])) - 5}개 더")
    
    print("\n" + "=" * 60)
    print("🎯 스키마 요약:")
    schema_summary = bq_client.get_schema_summary()
    print(schema_summary[:500] + "..." if len(schema_summary) > 500 else schema_summary)
    
    return True

def main():
    """메인 함수"""
    try:
        result = asyncio.run(test_bigquery_connection())
        if result:
            print("\n🎉 모든 테스트가 성공적으로 완료되었습니다!")
            print("이제 SQL Generator 워크플로우를 사용할 준비가 되었습니다.")
        else:
            print("\n❌ 테스트 중 문제가 발생했습니다.")
            print("위의 해결 방법을 참고하여 설정을 확인해주세요.")
            
    except KeyboardInterrupt:
        print("\n👋 사용자가 테스트를 중단했습니다.")
    except Exception as e:
        print(f"\n💥 예상치 못한 오류가 발생했습니다: {str(e)}")
        print("스택 트레이스:")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()