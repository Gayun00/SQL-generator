"""
Schema Analyzer Agent - RAG와 LLM을 결합한 스키마 정보 검색 및 분석
"""

from typing import Dict, Any, List, Optional
import json
import logging
import re

from rag.schema_retriever import schema_retriever
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage

# 로깅 설정
logger = logging.getLogger(__name__)

class SchemaAnalyzerAgent:
    """RAG와 LLM을 사용하여 관련 스키마 정보를 검색하고 분석하는 에이전트"""
    
    def __init__(self, similarity_threshold: float = 0.3, max_tables: int = 7, model_name: str = "gpt-4-turbo"):
        """
        SchemaAnalyzer Agent 초기화
        
        Args:
            similarity_threshold: 유사도 임계값
            max_tables: 최대 검색할 테이블 수
            model_name: 사용할 LLM 모델명
        """
        print("🔍 SchemaAnalyzer Agent 초기화")
        self.similarity_threshold = similarity_threshold
        self.max_tables = max_tables
        self.schema_retriever = schema_retriever
        self._initialized = False
        self.llm = ChatOpenAI(model=model_name, temperature=0.1)
    
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
            
            if not self._initialized:
                if not await self._initialize_retriever():
                    return {"success": False, "error": "Schema Retriever 초기화 실패", "schema_info": []}
            
            relevant_tables = self._search_relevant_schemas(user_query)
            
            if not relevant_tables:
                print("⚠️ 관련 스키마 정보를 찾을 수 없습니다.")
                return {"success": True, "schema_info": [], "message": "관련 스키마 정보를 찾을 수 없습니다."}
            
            analysis_result = await self._perform_relevance_analysis(user_query, relevant_tables)
            
            print(f"✅ 스키마 분석 완료: {len(analysis_result.get('schema_info', []))}개 테이블")
            
            return analysis_result
            
        except Exception as e:
            error_msg = f"스키마 분석 중 오류: {str(e)}"
            print(f"❌ {error_msg}")
            logger.error(error_msg, exc_info=True)
            return {"success": False, "error": error_msg, "schema_info": []}

    async def _perform_relevance_analysis(self, user_query: str, tables: List[Dict[str, Any]]) -> Dict[str, Any]:
        """LLM을 이용한 관련성 및 의도 심층 분석"""
        
        schema_info_str = self._format_schema_info_for_llm(tables)
        
        system_prompt = f"""
        당신은 자연어 쿼리와 데이터베이스 스키마를 분석하는 전문 AI Agent입니다.
        사용자의 자연어 쿼리와 RAG로 추출된 스키마 정보를 분석하여 SQL 생성에 필요한 구조화된 컨텍스트를 만드세요.

        **분석 프로세스:**
        1. **쿼리 의도 파악 (Intent Analysis):** 사용자가 무엇을 원하는지 명확히 분석합니다. (예: COUNT, SUM, AVG, 특정 데이터 조회 등)
        2. **필터 조건 식별 (Filter Identification):** 쿼리에 포함된 시간, 상태, 특정 값 등 모든 필터링 조건을 식별합니다. (예: 최근 7일, 특정 사용자 등)
        3. **스키마 관련성 평가 (Schema Relevance Assessment):** 제공된 스키마 정보 중 사용자의 의도와 직접적으로 관련된 테이블과 **특정 컬럼들**을 식별합니다. 관련 없는 정보는 과감히 제외합니다.
        4. **최종 컨텍스트 구성**: 분석 결과를 바탕으로, SQL 생성에 필요한 모든 정보를 포함한 최종 JSON을 반환합니다.

        **사용자 쿼리:** {user_query}

        **RAG로 추출된 스키마 정보:**
        {schema_info_str}

        **응답 형식 (JSON):**
        - 반드시 아래의 JSON 형식만으로 응답해야 합니다. 다른 설명은 절대 포함하지 마세요.
        - `schema_info` 필드에는 최종적으로 관련 있다고 판단된 테이블의 정보만 포함합니다.
        - `relevant_columns`에는 해당 테이블의 모든 컬럼이 아닌, **쿼리와 직접 관련된 컬럼만** 포함해야 합니다.

        ```json
        {{
            "success": true,
            "query_analysis": {{
                "user_query": "{user_query}",
                "intent": "사용자 의도(예: COUNT, SUM, SELECT)",
                "filters": [
                    {{"type": "date_range", "period": "last_7_days", "column": "적용할 날짜 컬럼명"}},
                    {{"type": "value_filter", "column": "필터링할 컬럼명", "value": "필터링 값"}}
                ],
                "natural_language_description": "LLM이 이해한 사용자의 요청 내용 요약"
            }},
            "schema_info": [
                {{
                    "table_name": "관련 테이블명",
                    "description": "테이블 설명",
                    "relevant_columns": [
                        {{"name": "관련 컬럼명1", "type": "데이터타입", "description": "컬럼 설명"}},
                        {{"name": "관련 컬럼명2", "type": "데이터타입", "description": "컬럼 설명"}}
                    ]
                }}
            ],
            "message": "Schema analysis completed successfully."
        }}
        ```
        """
        
        try:
            response = await self.llm.ainvoke([SystemMessage(content=system_prompt)])
            parsed_response = self._parse_json_response(response.content)
            
            if not parsed_response or not parsed_response.get("success"):
                return self._create_fallback_response(tables)
            
            return parsed_response

        except Exception as e:
            logger.error(f"LLM 관련성 분석 실패: {str(e)}", exc_info=True)
            return self._create_fallback_response(tables)

    def _format_schema_info_for_llm(self, tables: List[Dict[str, Any]]) -> str:
        """LLM 분석을 위해 테이블 정보를 문자열로 포맷"""
        # ... (이전과 동일, 변경 없음) ...
        formatted_info = []
        for i, table in enumerate(tables, 1):
            table_name = table.get("table_name", f"table_{i}")
            description = table.get("description", "")
            columns = table.get("columns", [])
            
            schema_text = f"{i}. 테이블: {table_name}\n   설명: {description}\n   컬럼:\n"
            for col in columns:
                field_text = f"     - {col.get('name')} ({col.get('type')}): {col.get('description')}"
                schema_text += field_text + "\n"
            formatted_info.append(schema_text)
        
        return "\n".join(formatted_info)

    def _parse_json_response(self, response_content: str) -> Optional[Dict]:
        """LLM의 JSON 응답을 파싱"""
        # ... (이전과 동일, 변경 없음) ...
        try:
            match = re.search(r"```json\n(.*?)\n```", response_content, re.DOTALL)
            if match:
                content = match.group(1)
            else:
                content = response_content
            
            return json.loads(content.strip())
        except json.JSONDecodeError as e:
            logger.warning(f"JSON 파싱 실패: {str(e)}\n원본 내용: {response_content[:200]}...")
            return None

    def _create_fallback_response(self, tables: List[Dict[str, Any]]) -> Dict[str, Any]:
        """LLM 분석 실패 시, RAG 결과 기반의 기본 응답 생성"""
        # ... (이전과 동일, 변경 없음) ...
        print("⚠️ LLM 분석 실패. RAG 검색 결과로 대체합니다.")
        return {
            "success": True,
            "schema_info": self._process_schema_info(tables),
            "message": "LLM 분석에 실패하여, 검색된 스키마 정보를 기반으로 결과를 제공합니다."
        }

    async def _initialize_retriever(self) -> bool:
        """Schema Retriever 초기화"""
        # ... (이전과 동일, 변경 없음) ...
        try:
            print("🚀 Schema Retriever 초기화 중...")
            if self.schema_retriever.initialize():
                self._initialized = True
                print("✅ Schema Retriever 초기화 완료")
                return True
            return False
        except Exception as e:
            print(f"❌ Schema Retriever 초기화 오류: {str(e)}")
            return False
    
    def _search_relevant_schemas(self, user_query: str) -> List[Dict]:
        """관련 스키마 정보 검색"""
        # ... (이전과 동일, 변경 없음) ...
        try:
            return self.schema_retriever.get_relevant_tables_with_threshold(
                query=user_query,
                top_k=self.max_tables,
                similarity_threshold=self.similarity_threshold
            )
        except Exception as e:
            print(f"❌ 스키마 검색 중 오류: {str(e)}")
            return []
    
    def _process_schema_info(self, schema_info: List[Dict]) -> List[Dict]:
        """스키마 정보 후처리 및 정제"""
        # ... (이전과 동일, 변경 없음) ...
        processed_schemas = []
        for table_info in schema_info:
            processed_table = {
                "table_name": table_info.get("table_name", ""),
                "description": table_info.get("description", ""),
                "columns": table_info.get("columns", [])
            }
            processed_schemas.append(processed_table)
        return processed_schemas

# 전역 SchemaAnalyzer 인스턴스
schema_analyzer_agent = SchemaAnalyzerAgent()