"""
SQL Executor Agent - BigQuery를 통한 SQL 실행
"""

from typing import Dict, Any, Optional
from db.bigquery_client import bq_client


class SQLExecutorAgent:
    """BigQuery를 사용하여 SQL 쿼리를 실행하는 에이전트"""
    
    def __init__(self, max_results: int = 100):
        """
        SQLExecutor Agent 초기화
        
        Args:
            max_results: 최대 결과 행 수
        """
        print("📊 SQLExecutor Agent 초기화")
        self.max_results = max_results
        self.bq_client = bq_client
        self._connected = False
    
    async def execute_query(self, sql_query: str, max_results: Optional[int] = None) -> Dict[str, Any]:
        """
        SQL 쿼리 실행
        
        Args:
            sql_query: 실행할 SQL 쿼리
            max_results: 최대 결과 행 수 (기본값 사용 시 None)
            
        Returns:
            쿼리 실행 결과
        """
        try:
            print(f"📊 SQL 실행 시작")
            print(f"📋 Query: {sql_query[:100]}{'...' if len(sql_query) > 100 else ''}")
            
            # BigQuery 클라이언트 연결 확인
            if not self._connected:
                if not await self._ensure_connection():
                    return {
                        "success": False,
                        "error": "BigQuery 연결에 실패했습니다.",
                        "results": [],
                        "query": sql_query
                    }
            
            # SQL 쿼리 검증
            validation_result = self._validate_query(sql_query)
            if not validation_result["valid"]:
                return {
                    "success": False,
                    "error": f"쿼리 검증 실패: {validation_result['error']}",
                    "results": [],
                    "query": sql_query
                }
            
            # 쿼리 실행
            execution_max_results = max_results or self.max_results
            result = self.bq_client.execute_query(sql_query, execution_max_results)
            
            if not result.get("success", False):
                print(f"❌ SQL 실행 실패: {result.get('error', '알 수 없는 오류')}")
                return {
                    "success": False,
                    "error": result.get("error", "SQL 실행 실패"),
                    "error_type": result.get("error_type", "unknown"),
                    "suggestion": result.get("suggestion", "쿼리를 확인해주세요."),
                    "results": [],
                    "query": sql_query
                }
            
            # 실행 결과 처리
            processed_result = self._process_execution_result(result)
            
            print(f"✅ SQL 실행 완료: {processed_result.get('returned_rows', 0)}개 결과")
            
            return processed_result
            
        except Exception as e:
            error_msg = f"SQL 실행 중 오류: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                "success": False,
                "error": error_msg,
                "results": [],
                "query": sql_query
            }
    
    async def _ensure_connection(self) -> bool:
        """BigQuery 연결 확인 및 설정"""
        try:
            print("🔌 BigQuery 연결 확인 중...")
            
            if self.bq_client.connect():
                self._connected = True
                print("✅ BigQuery 연결 완료")
                return True
            else:
                print("❌ BigQuery 연결 실패")
                return False
                
        except Exception as e:
            print(f"❌ BigQuery 연결 중 오류: {str(e)}")
            return False
    
    def _validate_query(self, sql_query: str) -> Dict[str, Any]:
        """SQL 쿼리 검증"""
        validation = {
            "valid": True,
            "error": None
        }
        
        try:
            # 기본 검증
            if not sql_query or not sql_query.strip():
                validation["valid"] = False
                validation["error"] = "빈 쿼리입니다."
                return validation
            
            # SELECT 문인지 확인
            query_upper = sql_query.strip().upper()
            if not query_upper.startswith("SELECT"):
                validation["valid"] = False
                validation["error"] = "SELECT 문만 실행 가능합니다."
                return validation
            
            # 위험한 키워드 체크
            dangerous_keywords = [
                "DROP", "DELETE", "UPDATE", "INSERT", "TRUNCATE", 
                "ALTER", "CREATE", "REPLACE"
            ]
            
            for keyword in dangerous_keywords:
                if keyword in query_upper:
                    validation["valid"] = False
                    validation["error"] = f"보안상 '{keyword}' 명령어는 사용할 수 없습니다."
                    return validation
            
            # 기본 구문 검증
            if "FROM" not in query_upper:
                validation["valid"] = False
                validation["error"] = "FROM 절이 없습니다."
                return validation
            
            return validation
            
        except Exception as e:
            validation["valid"] = False
            validation["error"] = f"쿼리 검증 중 오류: {str(e)}"
            return validation
    
    def _process_execution_result(self, raw_result: Dict) -> Dict[str, Any]:
        """실행 결과 후처리"""
        try:
            processed = {
                "success": raw_result.get("success", False),
                "results": raw_result.get("results", []),
                "total_rows": raw_result.get("total_rows", 0),
                "returned_rows": raw_result.get("returned_rows", 0),
                "bytes_processed": raw_result.get("bytes_processed", 0),
                "query": raw_result.get("query", ""),
                "truncated": raw_result.get("truncated", False),
                "execution_time": self._calculate_execution_time(),
                "summary": self._create_result_summary(raw_result)
            }
            
            # 에러 정보 포함 (있는 경우)
            if not processed["success"]:
                processed.update({
                    "error": raw_result.get("error", "알 수 없는 오류"),
                    "error_type": raw_result.get("error_type", "unknown"),
                    "suggestion": raw_result.get("suggestion", "쿼리를 확인해주세요.")
                })
            
            return processed
            
        except Exception as e:
            return {
                "success": False,
                "error": f"결과 처리 중 오류: {str(e)}",
                "results": [],
                "query": raw_result.get("query", "")
            }
    
    def _calculate_execution_time(self) -> str:
        """실행 시간 계산 (간단한 구현)"""
        # 실제로는 시작/종료 시간을 측정해야 함
        return "< 1초"
    
    def _create_result_summary(self, result: Dict) -> str:
        """실행 결과 요약 생성"""
        try:
            if not result.get("success", False):
                return f"실행 실패: {result.get('error', '알 수 없는 오류')}"
            
            returned_rows = result.get("returned_rows", 0)
            total_rows = result.get("total_rows", 0)
            bytes_processed = result.get("bytes_processed", 0)
            
            summary_parts = []
            summary_parts.append(f"✅ 쿼리 실행 완료")
            summary_parts.append(f"📊 결과: {returned_rows}개 행")
            
            if total_rows != returned_rows:
                summary_parts.append(f"📈 전체: {total_rows}개 행")
            
            if bytes_processed > 0:
                if bytes_processed > 1024 * 1024:  # MB
                    mb_processed = bytes_processed / (1024 * 1024)
                    summary_parts.append(f"💾 처리량: {mb_processed:.1f}MB")
                else:  # KB
                    kb_processed = bytes_processed / 1024
                    summary_parts.append(f"💾 처리량: {kb_processed:.1f}KB")
            
            if result.get("truncated", False):
                summary_parts.append("⚠️ 결과가 제한되었습니다")
            
            return " | ".join(summary_parts)
            
        except Exception as e:
            return f"요약 생성 실패: {str(e)}"
    
    def get_execution_stats(self) -> Dict[str, Any]:
        """실행 통계 정보 반환"""
        return {
            "max_results": self.max_results,
            "connected": self._connected,
            "client_info": {
                "project_id": getattr(self.bq_client, "project_id", ""),
                "default_dataset": getattr(self.bq_client, "default_dataset", "")
            }
        }
    
    def set_max_results(self, max_results: int):
        """최대 결과 수 설정"""
        if max_results > 0:
            self.max_results = max_results
            print(f"🔧 최대 결과 수 변경: {max_results}")
        else:
            print("❌ 최대 결과 수는 1 이상이어야 합니다.")


# 전역 SQLExecutor 인스턴스
sql_executor_agent = SQLExecutorAgent()