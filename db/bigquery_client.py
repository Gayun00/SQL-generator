from google.cloud import bigquery
from google.oauth2 import service_account
from core.config import BIGQUERY_CONFIG
import json
import os
from typing import Dict, List, Optional

class BigQueryClient:
    def __init__(self):
        self.keyfile_path = BIGQUERY_CONFIG["keyfile_path"]
        self.default_dataset = BIGQUERY_CONFIG["default_dataset"]
        self.target_tables = [table.strip() for table in BIGQUERY_CONFIG["target_tables"] if table.strip()]
        self.project_id = None  # keyfile에서 추출
        self.client = None
        self.schema_info = {}
        self.full_dataset_path = None  # project_id.dataset 형식
        
    def connect(self) -> bool:
        """BigQuery 클라이언트 연결"""
        try:
            # keyfile.json이 존재하는지 확인
            if not os.path.exists(self.keyfile_path):
                print(f"❌ keyfile을 찾을 수 없습니다: {self.keyfile_path}")
                print("💡 keyfile.json을 프로젝트 루트 디렉토리에 배치하세요.")
                return False
            
            # keyfile에서 project_id 추출
            with open(self.keyfile_path, 'r') as f:
                keyfile_data = json.load(f)
                self.project_id = keyfile_data.get('project_id')
                
            if not self.project_id:
                print("❌ keyfile.json에서 project_id를 찾을 수 없습니다.")
                print("💡 올바른 서비스 계정 키 파일인지 확인하세요.")
                return False
            
            # 서비스 계정 키 파일로 인증
            credentials = service_account.Credentials.from_service_account_file(
                self.keyfile_path
            )
            
            self.client = bigquery.Client(
                project=self.project_id,
                credentials=credentials
            )
            
            # 완전한 데이터셋 경로 설정
            if self.default_dataset:
                self.full_dataset_path = f"{self.project_id}.{self.default_dataset}"
            
            # 연결 테스트
            test_query = "SELECT 1 as test_connection"
            self.client.query(test_query).result()
            print(f"✅ BigQuery 연결 성공: {self.project_id}")
            print(f"📁 사용된 keyfile: {self.keyfile_path}")
            if self.full_dataset_path:
                print(f"📊 기본 데이터셋 경로: {self.full_dataset_path}")
            return True
            
        except FileNotFoundError:
            print(f"❌ keyfile을 찾을 수 없습니다: {self.keyfile_path}")
            print("💡 Google Cloud Console에서 서비스 계정 키를 다운로드하여 keyfile.json으로 저장하세요.")
            return False
        except json.JSONDecodeError:
            print(f"❌ keyfile 형식이 올바르지 않습니다: {self.keyfile_path}")
            print("💡 올바른 JSON 형식의 서비스 계정 키 파일인지 확인하세요.")
            return False
        except Exception as e:
            print(f"❌ BigQuery 연결 실패: {str(e)}")
            print("💡 확인사항:")
            print("  1. keyfile.json의 서비스 계정에 BigQuery 권한이 있는지 확인")
            print("  2. BigQuery API가 활성화되어 있는지 확인")
            return False
    
    def get_dataset_tables(self, dataset_id: str) -> List[str]:
        """데이터셋의 모든 테이블 목록 조회"""
        try:
            dataset_ref = self.client.dataset(dataset_id)
            tables = list(self.client.list_tables(dataset_ref))
            table_list = [table.table_id for table in tables]
            print(f"     📊 발견된 테이블: {table_list}")
            return table_list
        except Exception as e:
            error_msg = str(e)
            print(f"     ❌ 테이블 목록 조회 실패 ({dataset_id}): {error_msg}")
            
            # 구체적인 에러 유형별 안내
            if "404" in error_msg or "Not found" in error_msg:
                print(f"     💡 데이터셋 '{dataset_id}'가 존재하지 않습니다.")
                print(f"     💡 Google Cloud Console에서 데이터셋 이름을 확인하세요.")
            elif "403" in error_msg or "Access Denied" in error_msg:
                print(f"     💡 서비스 계정에 데이터셋 '{dataset_id}' 접근 권한이 없습니다.")
                print(f"     💡 BigQuery Data Viewer 또는 BigQuery User 역할을 부여하세요.")
            else:
                print(f"     💡 예상치 못한 오류입니다. 네트워크나 API 상태를 확인하세요.")
            
            return []
    
    def get_table_schema(self, dataset_id: str, table_id: str) -> Dict:
        """특정 테이블의 스키마 정보 조회"""
        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            table = self.client.get_table(table_ref)
            
            schema_info = {
                "table_name": f"{dataset_id}.{table_id}",
                "description": table.description or "",
                "columns": []
            }
            
            for field in table.schema:
                column_info = {
                    "name": field.name,
                    "type": field.field_type,
                    "mode": field.mode,
                    "description": field.description or ""
                }
                schema_info["columns"].append(column_info)
            
            return schema_info
            
        except Exception as e:
            error_msg = str(e)
            print(f"     ❌ 테이블 스키마 조회 실패 ({dataset_id}.{table_id}): {error_msg}")
            
            # 구체적인 에러 유형별 안내
            if "404" in error_msg or "Not found" in error_msg:
                print(f"     💡 테이블 '{dataset_id}.{table_id}'가 존재하지 않습니다.")
                print(f"     💡 Google Cloud Console에서 테이블 이름을 확인하세요.")
            elif "403" in error_msg or "Access Denied" in error_msg:
                print(f"     💡 서비스 계정에 테이블 '{dataset_id}.{table_id}' 접근 권한이 없습니다.")
                print(f"     💡 BigQuery Data Viewer 역할이 필요합니다.")
            else:
                print(f"     💡 예상치 못한 오류입니다. 네트워크나 API 상태를 확인하세요.")
                
            return {}
    
    def initialize_schema(self) -> Dict:
        """스키마 정보 초기화"""
        print("🔍 BigQuery 스키마 정보 수집 중...")
        
        if not self.client:
            print("❌ BigQuery 클라이언트가 연결되지 않았습니다.")
            return {}
        
        try:
            # 타겟 테이블이 지정된 경우
            if self.target_tables:
                print(f"📋 지정된 테이블들을 조회합니다: {self.target_tables}")
                for table_name in self.target_tables:
                    print(f"   🔍 처리 중: {table_name}")
                    if "." in table_name:
                        dataset_id, table_id = table_name.split(".", 1)
                    else:
                        dataset_id = self.default_dataset
                        table_id = table_name
                    
                    if not dataset_id:
                        print(f"   ⚠️ 데이터셋이 지정되지 않은 테이블: {table_name}")
                        print(f"   💡 {table_name}을 dataset.table 형식으로 지정하거나 BIGQUERY_DEFAULT_DATASET을 설정하세요.")
                        continue
                    
                    print(f"   📊 스키마 조회: {dataset_id}.{table_id}")
                    schema = self.get_table_schema(dataset_id, table_id)
                    if schema:
                        self.schema_info[f"{dataset_id}.{table_id}"] = schema
                        print(f"   ✅ 성공: {len(schema.get('columns', []))}개 컬럼")
            
            # 타겟 테이블이 지정되지 않은 경우 기본 데이터셋의 모든 테이블 조회
            elif self.default_dataset:
                print(f"📋 기본 데이터셋의 모든 테이블을 조회합니다: {self.default_dataset}")
                print(f"   🔍 데이터셋 테이블 목록 조회 중...")
                tables = self.get_dataset_tables(self.default_dataset)
                
                if not tables:
                    print(f"   ❌ 데이터셋 '{self.default_dataset}'에서 테이블을 찾을 수 없습니다.")
                    print(f"   💡 확인사항:")
                    print(f"     1. 데이터셋 이름이 정확한지 확인: {self.default_dataset}")
                    print(f"     2. 서비스 계정이 해당 데이터셋에 접근 권한이 있는지 확인")
                    print(f"     3. 데이터셋에 테이블이 존재하는지 확인")
                    return {}
                
                print(f"   📊 발견된 테이블 수: {len(tables)}")
                for table_id in tables:
                    print(f"   🔍 스키마 조회: {self.default_dataset}.{table_id}")
                    schema = self.get_table_schema(self.default_dataset, table_id)
                    if schema:
                        self.schema_info[f"{self.default_dataset}.{table_id}"] = schema
                        print(f"   ✅ 성공: {len(schema.get('columns', []))}개 컬럼")
            
            else:
                print("❌ 조회할 데이터셋 또는 테이블이 지정되지 않았습니다.")
                print("💡 해결방법:")
                print("   1. .env 파일에 BIGQUERY_DEFAULT_DATASET 설정")
                print("   2. 또는 BIGQUERY_TARGET_TABLES에 dataset.table 형식으로 테이블 지정")
                return {}
            
            if not self.schema_info:
                print("❌ 스키마 정보를 수집할 수 없습니다.")
                print("💡 가능한 원인:")
                print("   1. 지정된 데이터셋/테이블이 존재하지 않음")
                print("   2. 서비스 계정에 BigQuery Data Viewer 권한이 없음")
                print("   3. 잘못된 데이터셋/테이블 이름")
                return {}
            
            print(f"✅ 스키마 정보 수집 완료: {len(self.schema_info)}개 테이블")
            return self.schema_info
            
        except Exception as e:
            print(f"❌ 스키마 초기화 중 예상치 못한 오류: {str(e)}")
            print("💡 자세한 오류 정보:")
            import traceback
            traceback.print_exc()
            return {}
    
    def get_schema_summary(self) -> str:
        """스키마 정보를 문자열로 요약"""
        if not self.schema_info:
            return "스키마 정보가 없습니다."
        
        summary = []
        summary.append("=== BigQuery 테이블 스키마 정보 ===\n")
        
        for table_name, schema in self.schema_info.items():
            summary.append(f"📊 테이블: {table_name}")
            if schema.get("description"):
                summary.append(f"   설명: {schema['description']}")
            
            summary.append("   컬럼:")
            for col in schema.get("columns", []):
                col_desc = f" - {col['description']}" if col.get("description") else ""
                summary.append(f"     • {col['name']} ({col['type']}, {col['mode']}){col_desc}")
            summary.append("")
        
        return "\n".join(summary)
    
    def execute_query(self, query: str, max_results: int = 100) -> Dict:
        """SQL 쿼리 실행 및 결과 반환"""
        if not self.client:
            return {
                "success": False,
                "error": "BigQuery 클라이언트가 연결되지 않았습니다.",
                "results": []
            }
        
        try:
            print(f"🔍 쿼리 실행 중...")
            print(f"📋 Query: {query}")
            
            # 쿼리 실행
            query_job = self.client.query(query)
            
            # 쿼리 완료 대기
            query_result = query_job.result()
            
            # 결과 가져오기 (최대 결과 수 제한)
            results = []
            row_count = 0
            
            for row in query_result:
                if row_count >= max_results:
                    break
                    
                # Row를 딕셔너리로 변환
                row_dict = {}
                for key, value in row.items():
                    # BigQuery 특수 타입들을 Python 기본 타입으로 변환
                    if hasattr(value, 'isoformat'):  # datetime 객체
                        row_dict[key] = value.isoformat()
                    elif hasattr(value, '__iter__') and not isinstance(value, (str, bytes)):  # 리스트나 기타 iterable
                        row_dict[key] = list(value)
                    else:
                        row_dict[key] = value
                        
                results.append(row_dict)
                row_count += 1
            
            # 실행 통계 정보 (QueryJob에서 가져오기)
            total_rows = len(results)  # 기본적으로는 반환된 결과 수 사용
            bytes_processed = 0
            
            try:
                # QueryJob에서 통계 정보를 가져오려고 시도
                if hasattr(query_job, 'total_rows') and query_job.total_rows is not None:
                    total_rows = query_job.total_rows
                if hasattr(query_job, 'total_bytes_processed') and query_job.total_bytes_processed is not None:
                    bytes_processed = query_job.total_bytes_processed
            except AttributeError:
                # 속성이 없으면 기본값 사용
                pass
            
            print(f"✅ 쿼리 실행 완료!")
            print(f"   - 처리된 행 수: {len(results)}")
            print(f"   - 전체 행 수: {total_rows}")
            print(f"   - 처리된 바이트: {bytes_processed:,} bytes")
            
            if len(results) >= max_results and total_rows > max_results:
                print(f"⚠️ 결과가 {max_results}개로 제한되었습니다. (전체: {total_rows}개)")
            
            return {
                "success": True,
                "results": results,
                "total_rows": total_rows,
                "returned_rows": len(results),
                "bytes_processed": bytes_processed,
                "query": query,
                "truncated": len(results) >= max_results and total_rows > max_results
            }
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ 쿼리 실행 실패: {error_msg}")
            
            # 구체적인 에러 분석
            error_type = "unknown"
            suggestion = "쿼리 문법을 확인하세요."
            
            if "Syntax error" in error_msg or "Invalid" in error_msg:
                error_type = "syntax_error"
                suggestion = "SQL 문법을 확인하세요."
            elif "Table" in error_msg and "not found" in error_msg:
                error_type = "table_not_found"
                suggestion = "테이블 이름을 확인하세요. dataset.table 형식으로 작성했는지 확인하세요."
            elif "Column" in error_msg and "not found" in error_msg:
                error_type = "column_not_found" 
                suggestion = "컬럼 이름을 확인하세요."
            elif "Access Denied" in error_msg or "Permission" in error_msg:
                error_type = "permission_error"
                suggestion = "BigQuery 접근 권한을 확인하세요."
            elif "Query exceeded limit" in error_msg:
                error_type = "resource_limit"
                suggestion = "쿼리가 너무 복잡합니다. LIMIT을 추가하거나 조건을 추가하세요."
            
            return {
                "success": False,
                "error": error_msg,
                "error_type": error_type,
                "suggestion": suggestion,
                "query": query,
                "results": []
            }
    
    def get_full_table_path(self, table_name: str) -> str:
        """테이블명을 완전한 BigQuery 경로로 변환"""
        if not table_name:
            return ""
        
        # 이미 백틱으로 감싸져 있으면 그대로 반환
        if table_name.startswith('`') and table_name.endswith('`'):
            return table_name
        
        # 이미 프로젝트.데이터셋.테이블 형식이면 백틱만 추가
        if '.' in table_name and table_name.count('.') >= 2:
            return f"`{table_name}`"
        
        # 테이블명만 있는 경우 기본 데이터셋 경로 추가
        if self.full_dataset_path:
            if '.' in table_name:  # dataset.table 형식
                return f"`{self.project_id}.{table_name}`"
            else:  # table 형식
                return f"`{self.full_dataset_path}.{table_name}`"
        
        # 기본 데이터셋이 없으면 경고와 함께 원본 반환
        print(f"⚠️ 기본 데이터셋이 설정되지 않았습니다. 테이블명: {table_name}")
        return table_name
    
    def get_information_schema_path(self, schema_type: str = "COLUMNS") -> str:
        """INFORMATION_SCHEMA 경로 생성"""
        if self.full_dataset_path:
            return f"`{self.full_dataset_path}.INFORMATION_SCHEMA.{schema_type}`"
        return f"INFORMATION_SCHEMA.{schema_type}"

# 전역 클라이언트 인스턴스
bq_client = BigQueryClient()