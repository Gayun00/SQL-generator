"""
Schema Analyzer Agent - RAG와 LLM을 결합한 스키마 정보 검색, 분석 및 불확실성 정의
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
    """RAG와 LLM을 사용하여 관련 스키마를 분석하고 불확실성을 정의하는 에이전트"""
    
    def __init__(self, similarity_threshold: float = 0.3, max_tables: int = 7, model_name: str = "gpt-4-turbo"):
        print("🔍 SchemaAnalyzer Agent 초기화")
        self.similarity_threshold = similarity_threshold
        self.max_tables = max_tables
        self.schema_retriever = schema_retriever
        self._initialized = False
        self.llm = ChatOpenAI(model=model_name, temperature=0.1)
    
    async def analyze_query(self, user_query: str) -> Dict[str, Any]:
        """
        사용자 쿼리를 분석하여 관련 스키마 정보 검색 및 불확실성 정의
        """
        try:
            print(f"🔍 스키마 분석 시작: {user_query}")
            
            if not self._initialized:
                if not await self._initialize_retriever():
                    return {"success": False, "error": "Schema Retriever 초기화 실패"}
            
            relevant_tables = self._search_relevant_schemas(user_query)
            
            if not relevant_tables:
                print("⚠️ 관련 스키마 정보를 찾을 수 없습니다.")
                return {"success": True, "schema_info": [], "message": "관련 스키마 정보를 찾을 수 없습니다."}
            
            analysis_result = await self._perform_relevance_and_uncertainty_analysis(user_query, relevant_tables)
            
            if analysis_result.get("has_sufficient_info", True):
                print(f"✅ 스키마 분석 완료: {len(analysis_result.get('schema_info', []))}개 테이블")
            else:
                print(f"⚠️ 정보 불충분: {len(analysis_result.get('uncertainties', []))}개 불확실성 발견")

            return analysis_result
            
        except Exception as e:
            error_msg = f"스키마 분석 중 오류: {str(e)}"
            print(f"❌ {error_msg}")
            logger.error(error_msg, exc_info=True)
            return {"success": False, "error": error_msg}

    async def _perform_relevance_and_uncertainty_analysis(self, user_query: str, tables: List[Dict[str, Any]]) -> Dict[str, Any]:
        """LLM을 이용한 관련성, 의도, 불확실성 심층 분석"""
        
        schema_info_str = self._format_schema_info_for_llm(tables)
        
        system_prompt = f"""
        당신은 자연어 쿼리와 데이터베이스 스키마를 분석하여 SQL 생성 컨텍스트를 만드는 전문 AI Agent입니다.

        **분석 프로세스:**
        1. **쿼리 의도 및 필터 분석**: 사용자의 요청(Intent)과 필터링 조건(Filters)을 명확히 분석합니다.
        2. **스키마 관련성 평가**: 제공된 스키마 정보 중 사용자의 의도와 직접적으로 관련된 테이블과 컬럼을 식별합니다.
        3. **정보 충분성 판단**: 분석된 내용을 바탕으로, SQL 쿼리를 **오류 없이 정확하게** 생성하기에 정보가 충분한지 판단합니다.
        4. **불확실성 정의**: 정보가 불충분하다고 판단되면, 무엇이 모호하고 어떤 정보가 더 필요한지 `uncertainties` 목록으로 구체적으로 정의합니다. 각 불확실성은 `DataExplorerAgent`가 해결할 수 있는 구체적인 질문 형태여야 합니다.

        **사용자 쿼리:** {user_query}

        **RAG로 추출된 스키마 정보:**
        {schema_info_str}

        **응답 형식 (JSON):**
        - 반드시 아래의 JSON 형식만으로 응답해야 합니다. 다른 설명은 절대 포함하지 마세요.
        - 정보가 충분하면 `has_sufficient_info`를 `true`로, 불충분하면 `false`로 설정하세요.
        - `has_sufficient_info`가 `false`일 경우에만 `uncertainties` 필드를 채워주세요.

        ```json
        {{
            "success": true,
            "has_sufficient_info": true,
            "uncertainties": [
                {{
                    "type": "column_value_check",
                    "description": "users 테이블의 status 컬럼에 어떤 값들이 있는지 확인해야 합니다.",
                    "target_table": "users",
                    "target_column": "status"
                }},
                {{
                    "type": "data_format_check",
                    "description": "orders 테이블의 order_date 컬럼의 날짜 형식이 'YYYY-MM-DD'인지 확인이 필요합니다.",
                    "target_table": "orders",
                    "target_column": "order_date"
                }}
            ],
            "query_analysis": {{
                "user_query": "{user_query}",
                "intent": "사용자 의도(예: COUNT, SUM, SELECT)",
                "filters": [
                    {{"type": "date_range", "period": "last_7_days", "column": "적용할 날짜 컬럼명"}}
                ],
                "natural_language_description": "LLM이 이해한 사용자의 요청 내용 요약"
            }},
            "schema_info": [
                {{
                    "table_name": "관련 테이블명",
                    "description": "테이블 설명",
                    "relevant_columns": [
                        {{"name": "관련 컬럼명1", "type": "데이터타입", "description": "컬럼 설명"}}
                    ]
                }}
            ],
            "message": "분석 요약 메시지"
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
        print("⚠️ LLM 분석 실패. RAG 검색 결과로 대체합니다.")
        return {
            "success": True,
            "has_sufficient_info": True, # LLM 실패시 일단 충분한 것으로 간주
            "schema_info": self._process_schema_info(tables),
            "message": "LLM 분석에 실패하여, 검색된 스키마 정보를 기반으로 결과를 제공합니다."
        }

    async def _initialize_retriever(self) -> bool:
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
