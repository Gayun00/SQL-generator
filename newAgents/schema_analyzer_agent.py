"""
Schema Analyzer Agent - RAG를 통한 스키마 정보 검색 및 분석
"""

from typing import Dict, Any, List, Optional
from rag.schema_retriever import schema_retriever


class SchemaAnalyzerAgent:
    """RAG를 사용하여 관련 스키마 정보를 검색하고 분석하는 에이전트"""
    
    def __init__(self, similarity_threshold: float = 0.5, max_tables: int = 5):
        """
        SchemaAnalyzer Agent 초기화
        
        Args:
            similarity_threshold: 유사도 임계값
            max_tables: 최대 검색할 테이블 수
        """
        print("🔍 SchemaAnalyzer Agent 초기화")
        self.similarity_threshold = similarity_threshold
        self.max_tables = max_tables
        self.schema_retriever = schema_retriever
        self._initialized = False
    
    async def analyze_query(self, user_query: str) -> Dict[str, Any]:
        """
        사용자 쿼리를 분석하여 관련 스키마 정보 검색
        
        Args:
            user_query: 사용자 자연어 쿼리
            
        Returns:
            스키마 분석 결과
        """
        try:
            print(f"🔍 스키마 분석 시작: {user_query}")
            
            # Schema Retriever 초기화
            if not self._initialized:
                if not await self._initialize_retriever():
                    return {
                        "success": False,
                        "error": "Schema Retriever 초기화 실패",
                        "schema_info": []
                    }
            
            # 관련 스키마 정보 검색
            schema_info = self._search_relevant_schemas(user_query)
            
            if not schema_info:
                print("⚠️ 관련 스키마 정보를 찾을 수 없습니다.")
                return {
                    "success": True,
                    "schema_info": [],
                    "message": "관련 스키마 정보를 찾을 수 없습니다. 다른 키워드로 시도해보세요.",
                    "query": user_query
                }
            
            # 스키마 정보 후처리
            processed_schema = self._process_schema_info(schema_info)
            
            print(f"✅ 스키마 분석 완료: {len(processed_schema)}개 테이블")
            
            return {
                "success": True,
                "schema_info": processed_schema,
                "message": f"{len(processed_schema)}개의 관련 테이블을 찾았습니다.",
                "query": user_query,
                "similarity_threshold": self.similarity_threshold
            }
            
        except Exception as e:
            error_msg = f"스키마 분석 중 오류: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                "success": False,
                "error": error_msg,
                "schema_info": []
            }
    
    async def _initialize_retriever(self) -> bool:
        """Schema Retriever 초기화"""
        try:
            print("🚀 Schema Retriever 초기화 중...")
            if self.schema_retriever.initialize():
                self._initialized = True
                print("✅ Schema Retriever 초기화 완료")
                return True
            else:
                print("❌ Schema Retriever 초기화 실패")
                return False
        except Exception as e:
            print(f"❌ Schema Retriever 초기화 오류: {str(e)}")
            return False
    
    def _search_relevant_schemas(self, user_query: str) -> List[Dict]:
        """관련 스키마 정보 검색"""
        try:
            # 임계값 기반 스키마 검색
            relevant_tables = self.schema_retriever.get_relevant_tables_with_threshold(
                query=user_query,
                top_k=self.max_tables,
                similarity_threshold=self.similarity_threshold
            )
            
            return relevant_tables
            
        except Exception as e:
            print(f"❌ 스키마 검색 중 오류: {str(e)}")
            return []
    
    def _process_schema_info(self, schema_info: List[Dict]) -> List[Dict]:
        """스키마 정보 후처리 및 정제"""
        processed_schemas = []
        
        for table_info in schema_info:
            # 기본 테이블 정보
            processed_table = {
                "table_name": table_info.get("table_name", ""),
                "dataset": table_info.get("dataset", ""),
                "table_id": table_info.get("table_id", ""),
                "description": table_info.get("description", ""),
                "columns": [],
                "relevance_score": table_info.get("relevance_score", 0),
                "matched_elements": table_info.get("matched_elements", [])
            }
            
            # 컬럼 정보 처리
            for column in table_info.get("columns", []):
                processed_column = {
                    "name": column.get("name", ""),
                    "type": column.get("type", ""),
                    "mode": column.get("mode", "NULLABLE"),
                    "description": column.get("description", "")
                }
                processed_table["columns"].append(processed_column)
            
            processed_schemas.append(processed_table)
        
        return processed_schemas
    
    def get_schema_summary(self, schema_info: List[Dict]) -> str:
        """스키마 정보를 요약 문자열로 변환"""
        if not schema_info:
            return "관련 스키마 정보가 없습니다."
        
        summary_parts = []
        summary_parts.append("🔍 발견된 관련 테이블:")
        
        for i, table in enumerate(schema_info, 1):
            table_name = table.get("table_name", "")
            description = table.get("description", "")
            column_count = len(table.get("columns", []))
            
            summary_parts.append(f"\n{i}. 📊 {table_name}")
            if description:
                summary_parts.append(f"   설명: {description}")
            summary_parts.append(f"   컬럼: {column_count}개")
            
            # 주요 컬럼 표시 (최대 5개)
            columns = table.get("columns", [])[:5]
            if columns:
                column_names = [col.get("name", "") for col in columns]
                summary_parts.append(f"   주요 컬럼: {', '.join(column_names)}")
                if len(table.get("columns", [])) > 5:
                    summary_parts.append(f"   ... (총 {len(table.get('columns', []))}개)")
        
        return "\n".join(summary_parts)
    
    def adjust_similarity_threshold(self, new_threshold: float):
        """유사도 임계값 조정"""
        if 0.0 <= new_threshold <= 1.0:
            self.similarity_threshold = new_threshold
            print(f"🔧 유사도 임계값 변경: {new_threshold}")
        else:
            print("❌ 유사도 임계값은 0.0~1.0 사이여야 합니다.")


# 전역 SchemaAnalyzer 인스턴스
schema_analyzer_agent = SchemaAnalyzerAgent()