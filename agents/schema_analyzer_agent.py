# -*- coding: utf-8 -*-
"""
SchemaAnalyzer Agent - 자연어 텍스트와 스키마 정보 분석 전문 Agent

사용자의 자연어 요청을 분석하여 관련 스키마 정보를 추출하고,
불충분한 정보에 대해서는 추가 정보를 요청하는 에이전트
"""

from typing import Dict, Any, List, Optional, Literal
import logging
from datetime import datetime
import json
from dataclasses import dataclass, asdict

# 직접 실행시 import 오류 방지를 위한 경로 설정
import sys
import os
if __name__ == "__main__":
    # 프로젝트 루트를 Python 경로에 추가
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, project_root)

from agents.simple_base_agent import SimpleBaseAgent, AgentMessage, MessageType, AgentConfig, create_agent_config

logger = logging.getLogger(__name__)
# RAG schema retriever import
from rag.schema_retriever import schema_retriever

@dataclass
class SchemaAnalyzerInput:
    """SchemaAnalyzer 입력 데이터 구조"""
    userInput: str  # 사용자가 자연어로 쓴 질문

@dataclass
class SchemaAnalyzerOutput:
    """SchemaAnalyzer 출력 데이터 구조"""
    analysis_type: Literal["schema_context", "needs_more_info"]
    # schema_context인 경우
    relevantTables: Optional[List[str]] = None
    relevantFields: Optional[Dict[str, List[str]]] = None
    joins: Optional[List[Dict[str, str]]] = None  # [{"from": str, "to": str}]
    naturalDescription: Optional[str] = None
    # needs_more_info인 경우
    missingInfoDescription: Optional[str] = None
    followUpQuestions: Optional[List[str]] = None

class SchemaAnalyzerAgent(SimpleBaseAgent):
    """자연어 텍스트와 스키마 정보 분석 전문 Agent"""
    
    def __init__(self, config: Optional[AgentConfig] = None, similarity_threshold: float = 0.3):
        if config is None:
            config = create_agent_config(
                name="schema_analyzer",
                specialization="schema_analysis_and_context_extraction",
                model="gpt-4",
                temperature=0.3,
                max_tokens=1500
            )
        
        super().__init__(config)
        self.similarity_threshold = similarity_threshold  # 유사도 임계값 설정
        logger.info(f"SchemaAnalyzer Agent initialized with similarity_threshold={similarity_threshold}")
    
    def get_system_prompt(self) -> str:
        """스키마 분석 전문 시스템 프롬프트"""
        return """
        당신은 자연어 쿼리와 데이터베이스 스키마 정보를 분석하는 전문 AI Agent입니다.
        
        **핵심 역할:**
        1. 사용자의 자연어 요청을 분석하여 필요한 테이블과 필드 식별
        2. 임베딩된 스키마 정보에서 관련성 높은 요소들 추출
        3. 테이블 간 조인 관계 파악 및 제안
        4. 정보가 불충분한 경우 구체적인 추가 정보 요청
        
        **분석 원칙:**
        - 자연어 쿼리의 핵심 의도 파악
        - 스키마와 쿼리 간의 의미적 연관성 분석
        - 모호한 부분에 대해서는 명확한 추가 질문 생성
        - BigQuery 환경을 고려한 스키마 분석
        """
    
    async def process_message(self, message: AgentMessage) -> AgentMessage:
        """메시지 처리 - 스키마 분석"""
        try:
            # 입력 데이터 파싱
            content = message.content
            input_data = SchemaAnalyzerInput(
                userInput=content.get("userInput")
            )
            
            # 스키마 분석 수행
            result = await self._analyze_schema_context(input_data)
            
            # 결과 반환
            return self.create_response_message(message, asdict(result))
            
        except Exception as e:
            logger.error(f"SchemaAnalyzer processing failed: {str(e)}")
            return self.create_error_message(message, e)
    
    async def _analyze_schema_context(self, input_data: SchemaAnalyzerInput) -> SchemaAnalyzerOutput:
        """스키마 컨텍스트 분석"""
        logger.info(f"Analyzing schema context for query: {input_data.userInput[:50]}...")
        
        # 1. 입력 유효성 검사
        if not input_data.userInput or not input_data.userInput.strip():
            return SchemaAnalyzerOutput(
                analysis_type="needs_more_info",
                missingInfoDescription="사용자 입력이 비어있습니다.",
                followUpQuestions=["어떤 데이터를 조회하고 싶으신지 구체적으로 알려주세요."]
            )
        
        # 2. RAG를 통해 관련 스키마 정보 검색
        try:
            # schema_retriever 초기화 확인
            if not schema_retriever.vectorstore:
                if not schema_retriever.initialize():
                    return SchemaAnalyzerOutput(
                        analysis_type="needs_more_info",
                        missingInfoDescription="스키마 검색 시스템이 초기화되지 않았습니다.",
                        followUpQuestions=["데이터베이스 스키마를 먼저 로드해주세요."]
                    )
            
            # 관련 테이블 정보 검색 (유사도 임계값 적용)
            relevant_tables = schema_retriever.get_relevant_tables_with_threshold(
                input_data.userInput, 
                top_k=10, 
                similarity_threshold=self.similarity_threshold
            )
            
            if not relevant_tables:
                # 임계값을 낮춰서 재검색 시도
                logger.info(f"No tables found with threshold {self.similarity_threshold}, trying with lower threshold")
                fallback_tables = schema_retriever.get_relevant_tables_with_threshold(
                    input_data.userInput, 
                    top_k=5, 
                    similarity_threshold=0.1  # 매우 낮은 임계값으로 재시도
                )
                
                if fallback_tables:
                    # 낮은 유사도의 테이블들이 발견된 경우
                    available_tables = [table.get("table_name") for table in fallback_tables[:3]]
                    return SchemaAnalyzerOutput(
                        analysis_type="needs_more_info",
                        missingInfoDescription=f"입력한 질문과 충분히 관련된 스키마를 찾지 못했습니다 (유사도 임계값: {self.similarity_threshold}).",
                        followUpQuestions=[
                            f"혹시 다음 테이블들과 관련된 내용인가요? {', '.join(available_tables)}",
                            "더 구체적인 키워드나 테이블명을 포함해서 다시 질문해 주시겠어요?",
                            "어떤 종류의 데이터를 조회하고 싶으신지 자세히 설명해 주세요."
                        ]
                    )
                else:
                    # 아예 관련 테이블을 찾을 수 없는 경우
                    return SchemaAnalyzerOutput(
                        analysis_type="needs_more_info",
                        missingInfoDescription="입력한 질문과 관련된 스키마 정보를 찾을 수 없습니다.",
                        followUpQuestions=[
                            "다른 키워드나 표현을 사용해서 다시 질문해 주시겠어요?",
                            "구체적으로 어떤 데이터나 테이블에 대해 알고 싶으신가요?",
                            "데이터베이스에 어떤 종류의 테이블들이 있는지 먼저 확인해보시겠어요?"
                        ]
                    )
            
            # 3. 관련성 분석 수행
            analysis_result = await self._perform_relevance_analysis(
                input_data.userInput,
                relevant_tables
            )
            
            return analysis_result
            
        except Exception as e:
            logger.error(f"Schema context analysis failed: {str(e)}")
            return SchemaAnalyzerOutput(
                analysis_type="needs_more_info",
                missingInfoDescription="스키마 분석 중 오류가 발생했습니다.",
                followUpQuestions=["다시 시도해 주시거나, 더 구체적인 정보를 알려주세요."]
            )
    
    async def _perform_relevance_analysis(self, user_input: str, tables: List[Dict[str, Any]]) -> SchemaAnalyzerOutput:
        """관련성 분석 수행"""
        
        # 테이블 정보를 문자열로 정리
        schema_info = self._format_schema_info(tables)
        
        system_prompt = f"""
        사용자의 자연어 쿼리와 데이터베이스 스키마를 분석하여 SQL 생성에 필요한 정보를 추출하세요.
        
        사용자 쿼리: {user_input}
        
        사용 가능한 스키마 정보:
        {schema_info}
        
        다음을 분석해주세요:
        1. 쿼리에서 언급된 데이터 요소들 (날짜, 금액, 사용자, 제품 등)
        2. 필요한 테이블들과 각 테이블의 관련 필드들
        3. 테이블 간 조인이 필요한 경우 조인 관계
        4. 추가 정보가 필요한지 여부
        
        응답 형식 (JSON):
        {{
            "has_sufficient_info": true/false,
            "relevant_tables": ["테이블1", "테이블2", ...],
            "relevant_fields": {{
                "테이블1": ["필드1", "필드2", ...],
                "테이블2": ["필드1", "필드2", ...]
            }},
            "suggested_joins": [
                {{"from": "테이블1", "to": "테이블2", "condition": "조인 조건"}}
            ],
            "natural_description": "분석 결과 요약",
            "missing_info": {{
                "description": "부족한 정보 설명",
                "questions": ["구체적인 질문1", "질문2", ...]
            }}
        }}
        """
        
        try:
            response_content = await self.send_llm_request(system_prompt)
            parsed_response = self._parse_json_response(response_content)
            
            if not parsed_response:
                # JSON 파싱 실패시 기본 응답
                return self._create_fallback_response(user_input, tables)
            
            # 충분한 정보가 있는 경우
            if parsed_response.get("has_sufficient_info", False):
                return SchemaAnalyzerOutput(
                    analysis_type="schema_context",
                    relevantTables=parsed_response.get("relevant_tables", []),
                    relevantFields=parsed_response.get("relevant_fields", {}),
                    joins=parsed_response.get("suggested_joins", []),
                    naturalDescription=parsed_response.get("natural_description", "")
                )
            else:
                # 추가 정보가 필요한 경우
                missing_info = parsed_response.get("missing_info", {})
                return SchemaAnalyzerOutput(
                    analysis_type="needs_more_info",
                    missingInfoDescription=missing_info.get("description", "추가 정보가 필요합니다."),
                    followUpQuestions=missing_info.get("questions", ["더 구체적인 정보를 알려주세요."])
                )
                
        except Exception as e:
            logger.error(f"Relevance analysis failed: {str(e)}")
            return self._create_fallback_response(user_input, tables)
    
    def _format_schema_info(self, tables: List[Dict[str, Any]]) -> str:
        """테이블 정보를 분석용 문자열로 포맷"""
        formatted_info = []
        
        for i, table in enumerate(tables[:10], 1):  # 최대 10개 테이블만 처리
            table_name = table.get("table_name", f"table_{i}")
            description = table.get("description", "")
            columns = table.get("columns", [])
            
            schema_text = f"{i}. 테이블: {table_name}"
            if description:
                schema_text += f"\n   설명: {description}"
            
            if columns:
                schema_text += f"\n   필드: "
                field_names = []
                for col in columns[:10]:  # 최대 10개 필드
                    col_name = col.get("name", "")
                    col_type = col.get("type", "")
                    col_desc = col.get("description", "")
                    
                    field_text = col_name
                    if col_type:
                        field_text += f"({col_type})"
                    if col_desc:
                        field_text += f" - {col_desc}"
                    field_names.append(field_text)
                
                schema_text += ", ".join(field_names)
                
                if len(columns) > 10:
                    schema_text += f" ... (+{len(columns)-10}개 더)"
            
            formatted_info.append(schema_text)
        
        return "\n\n".join(formatted_info)
    
    def _create_fallback_response(self, user_input: str, tables: List[Dict[str, Any]]) -> SchemaAnalyzerOutput:
        """분석 실패시 기본 응답 생성"""
        # 테이블에서 테이블명만 추출
        available_tables = []
        for table in tables[:5]:  # 최대 5개만
            table_name = table.get("table_name")
            if table_name and table_name not in available_tables:
                available_tables.append(table_name)
        
        return SchemaAnalyzerOutput(
            analysis_type="needs_more_info",
            missingInfoDescription="스키마 분석 중 오류가 발생했습니다.",
            followUpQuestions=[
                f"다음 테이블 중 어떤 것과 관련된 데이터를 원하시나요? {', '.join(available_tables)}",
                "조회하고 싶은 구체적인 기간이나 조건이 있나요?"
            ]
        )
    
    def _parse_json_response(self, response_content: str) -> Optional[Dict]:
        """JSON 응답 파싱"""
        try:
            content = response_content.strip()
            if content.startswith("```json"):
                content = content[7:]
            if content.startswith("```"):
                content = content[3:]
            if content.endswith("```"):
                content = content[:-3]
            
            return json.loads(content.strip())
        except json.JSONDecodeError as e:
            logger.warning(f"JSON parsing failed: {str(e)}")
            return None


# Agent 생성 헬퍼 함수
def create_schema_analyzer_agent(custom_config: Optional[Dict[str, Any]] = None, similarity_threshold: float = 0.3) -> SchemaAnalyzerAgent:
    """SchemaAnalyzer Agent 생성"""
    config = create_agent_config(
        name="schema_analyzer",
        specialization="schema_analysis_and_context_extraction",
        model="gpt-4",
        temperature=0.3,
        max_tokens=1500,
        **(custom_config or {})
    )
    
    return SchemaAnalyzerAgent(config, similarity_threshold=similarity_threshold)