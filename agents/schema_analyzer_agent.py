"""
SchemaAnalyzer Agent - 스키마 분석 및 불확실성 탐지 전문 Agent

기존 sql_analyzer 노드를 Agent로 변환하여 스키마 패턴 인식, 불확실성 탐지,
데이터 관계 분석에 특화된 지능형 Agent로 구현했습니다.
"""

from typing import Dict, Any, List, Optional
import json
import logging
from datetime import datetime

from .base_agent import BaseAgent, AgentMessage, MessageType, AgentConfig, create_agent_config
from rag.schema_retriever import schema_retriever
from langchain.schema import HumanMessage, SystemMessage

logger = logging.getLogger(__name__)

class SchemaAnalyzerAgent(BaseAgent):
    """스키마 분석 및 불확실성 탐지 전문 Agent"""
    
    def __init__(self, config: Optional[AgentConfig] = None):
        if config is None:
            config = create_agent_config(
                name="schema_analyzer",
                specialization="schema_analysis_uncertainty_detection",
                model="gpt-4",
                temperature=0.2,  # 정확성 중시
                max_tokens=2000
            )
        
        super().__init__(config)
        
        # 스키마 분석 전용 설정
        self.uncertainty_types = {
            "column_values": "컬럼 값 불확실성",
            "table_relationship": "테이블 관계 불확실성", 
            "data_range": "데이터 범위 불확실성"
        }
        
        # 성능 추적
        self.analysis_history = []
        
        logger.info(f"SchemaAnalyzer Agent initialized with specialization: {self.specialization}")
    
    def get_system_prompt(self) -> str:
        """스키마 분석 전문 시스템 프롬프트"""
        return f"""
        당신은 SQL 스키마 분석과 불확실성 탐지 전문 AI Agent입니다.
        
        **전문 분야:**
        - 데이터베이스 스키마 패턴 인식
        - 테이블 간 관계 분석
        - 쿼리 불확실성 탐지
        - RAG 기반 스키마 정보 활용
        
        **핵심 역할:**
        1. 사용자 쿼리에서 불명확한 요소들을 정확히 식별
        2. 스키마 정보를 활용한 관계 분석
        3. 추가 탐색이 필요한 영역 제안
        
        **불확실성 유형:**
        1. column_values: 컬럼에 어떤 값들이 있는지 모르는 경우
           - 예: "상태가 '활성'인 사용자" → status 컬럼의 실제 값들 확인 필요
           - 예: "카테고리별 매출" → category 컬럼의 실제 값들 확인 필요
        
        2. table_relationship: 테이블 간 관계가 불분명한 경우
           - 예: "사용자별 주문 정보" → users와 orders 테이블의 연결 방법
           - 예: "상품과 주문의 관계" → 중간 테이블 존재 여부
        
        3. data_range: 데이터의 범위나 분포가 불분명한 경우
           - 예: "최근 데이터" → 실제 데이터의 날짜 범위
           - 예: "인기 상품" → 판매량이나 평점의 기준값
        
        **응답 원칙:**
        - 정확성과 신중함을 최우선으로 함
        - 스키마 정보를 기반으로 한 분석 제공
        - 불확실성이 있다면 반드시 탐지하여 보고
        - JSON 형식으로 구조화된 결과 제공
        
        **성능 최적화:**
        - 분석 시간: 평균 2-5초 목표
        - 정확도: 95% 이상 목표
        - 신뢰도 점수 제공으로 품질 보장
        """
    
    async def process_message(self, message: AgentMessage) -> AgentMessage:
        """메시지 처리 - 스키마 분석 작업 수행"""
        try:
            # 입력 유효성 검증
            if not await self.validate_input(message):
                return self.create_error_message(message, ValueError("Invalid input message"))
            
            # 메시지 히스토리에 추가
            self.add_message_to_history(message)
            
            # 작업 타입에 따른 처리
            task_type = message.content.get("task_type", "full_analysis")
            input_data = message.content.get("input_data", {})
            
            if task_type == "quick_analysis":
                result = await self._quick_analysis(input_data)
            elif task_type == "full_analysis":
                result = await self._full_analysis(input_data)
            elif task_type == "deep_analysis":
                result = await self._deep_analysis(input_data)
            elif task_type == "validation_review":
                result = await self._validation_review(input_data)
            else:
                result = await self._full_analysis(input_data)  # 기본값
            
            # 성공 응답 생성
            return self.create_response_message(message, result)
            
        except Exception as e:
            logger.error(f"SchemaIntelligence Agent processing failed: {str(e)}")
            return self.create_error_message(message, e)
    
    async def _quick_analysis(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """빠른 분석 - 단순한 쿼리용"""
        query = input_data.get("query", "")
        
        # 기본적인 키워드 기반 불확실성 체크
        uncertainty_keywords = {
            "column_values": ["상태", "카테고리", "유형", "타입", "활성", "비활성"],
            "table_relationship": ["별", "의", "간", "관계", "연결"],
            "data_range": ["최근", "이번", "지난", "많은", "적은", "높은", "낮은"]
        }
        
        detected_uncertainties = []
        
        for uncertainty_type, keywords in uncertainty_keywords.items():
            for keyword in keywords:
                if keyword in query:
                    detected_uncertainties.append({
                        "type": uncertainty_type,
                        "description": f"'{keyword}' 키워드로 인한 {self.uncertainty_types[uncertainty_type]}",
                        "confidence": 0.7,
                        "table": "unknown",
                        "column": "unknown" if uncertainty_type != "table_relationship" else None,
                        "exploration_query": f"-- {uncertainty_type} 탐색이 필요함"
                    })
                    break  # 타입별로 하나만 탐지
        
        has_uncertainty = len(detected_uncertainties) > 0
        
        result = {
            "analysis_type": "quick_analysis",
            "has_uncertainty": has_uncertainty,
            "uncertainties": detected_uncertainties,
            "confidence": 0.8 if not has_uncertainty else 0.6,
            "processing_time": 0.5,  # 빠른 분석
            "recommendation": "빠른 분석 완료. 복잡한 쿼리의 경우 full_analysis 권장"
        }
        
        return result
    
    async def _full_analysis(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """전체 분석 - 표준 복잡도 쿼리용"""
        query = input_data.get("query", "")
        state = input_data.get("state", {})
        
        logger.info(f"SchemaIntelligence: Full analysis started for query: '{query[:50]}...'")
        
        # RAG를 통한 관련 스키마 검색
        try:
            relevant_context = schema_retriever.create_context_summary(query, max_tables=5)
            logger.info("RAG based schema search completed")
        except Exception as e:
            logger.warning(f"RAG search failed: {str(e)}")
            relevant_context = "스키마 정보를 가져올 수 없습니다."
        
        # LLM을 통한 분석
        user_message = f"""
        사용자 요청: {query}
        
        현재 상태 정보:
        - 이전 분석 결과: {state.get('uncertaintyAnalysis', '없음')}
        - 탐색 결과: {state.get('explorationResults', '없음')}
        
        다음 스키마 정보를 참고하세요:
        {relevant_context}
        
        위 정보를 바탕으로 불확실성을 분석하고 JSON 형식으로 응답해주세요.
        
        응답 형식:
        {{
            "has_uncertainty": true/false,
            "uncertainties": [
                {{
                    "type": "column_values|table_relationship|data_range",
                    "description": "불확실성 설명",
                    "table": "관련 테이블명",
                    "column": "관련 컬럼명 (해당시)",
                    "exploration_query": "탐지를 위한 SQL 쿼리",
                    "confidence": 0.0-1.0
                }}
            ],
            "confidence": 0.0-1.0,
            "reasoning": "분석 근거"
        }}
        """
        
        try:
            start_time = datetime.now()
            response_content = await self.send_llm_request(user_message)
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # JSON 파싱
            analysis_result = self._parse_json_response(response_content)
            
            # 결과 보강
            analysis_result.update({
                "analysis_type": "full_analysis",
                "processing_time": processing_time,
                "schema_context_used": relevant_context is not None,
                "query_complexity": self._assess_query_complexity(query)
            })
            
            # 분석 히스토리에 추가
            self.analysis_history.append({
                "timestamp": datetime.now().isoformat(),
                "query": query,
                "has_uncertainty": analysis_result.get("has_uncertainty", False),
                "confidence": analysis_result.get("confidence", 0.0),
                "processing_time": processing_time
            })
            
            logger.info(f"Full analysis completed in {processing_time:.2f}s")
            return analysis_result
            
        except Exception as e:
            logger.error(f"Full analysis failed: {str(e)}")
            return self._create_fallback_result("full_analysis", str(e))
    
    async def _deep_analysis(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """심층 분석 - 복잡한 쿼리용"""
        query = input_data.get("query", "")
        state = input_data.get("state", {})
        
        logger.info(f"SchemaIntelligence: Deep analysis started for complex query")
        
        # 다단계 분석 수행
        
        # 1단계: 기본 분석
        basic_result = await self._full_analysis(input_data)
        
        # 2단계: 컨텍스트 확장 분석
        extended_context = await self._get_extended_schema_context(query)
        
        # 3단계: 관계 분석 강화
        relationship_analysis = await self._analyze_table_relationships(query, extended_context)
        
        # 4단계: 데이터 품질 분석
        data_quality_analysis = await self._analyze_data_quality_concerns(query)
        
        # 결과 통합
        deep_result = {
            **basic_result,
            "analysis_type": "deep_analysis",
            "extended_context": extended_context,
            "relationship_analysis": relationship_analysis,
            "data_quality_concerns": data_quality_analysis,
            "recommendation": self._generate_deep_analysis_recommendation(basic_result, relationship_analysis)
        }
        
        logger.info("Deep analysis completed")
        return deep_result
    
    async def _validation_review(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """검증 리뷰 - 생성된 SQL 검토"""
        draft_sql = input_data.get("draft_sql", "")
        original_query = input_data.get("original_query", "")
        
        logger.info("SchemaIntelligence: Validation review started")
        
        validation_prompt = f"""
        원본 사용자 요청: {original_query}
        생성된 SQL: {draft_sql}
        
        위 SQL이 사용자 요청을 정확히 반영하는지 스키마 관점에서 검증해주세요.
        
        검증 항목:
        1. 테이블 및 컬럼명 정확성
        2. JOIN 조건 적절성
        3. WHERE 조건 완전성
        4. 데이터 타입 일치성
        5. 성능 최적화 가능성
        
        JSON 응답 형식:
        {{
            "validation_passed": true/false,
            "issues": [
                {{
                    "type": "table|column|join|where|performance",
                    "severity": "critical|warning|info",
                    "description": "문제 설명",
                    "suggestion": "개선 제안"
                }}
            ],
            "confidence": 0.0-1.0,
            "overall_assessment": "종합 평가"
        }}
        """
        
        try:
            response_content = await self.send_llm_request(validation_prompt)
            validation_result = self._parse_json_response(response_content)
            
            validation_result.update({
                "analysis_type": "validation_review",
                "sql_reviewed": draft_sql,
                "original_query": original_query
            })
            
            return validation_result
            
        except Exception as e:
            logger.error(f"Validation review failed: {str(e)}")
            return self._create_fallback_validation_result(str(e))
    
    async def _get_extended_schema_context(self, query: str) -> Dict[str, Any]:
        """확장된 스키마 컨텍스트 수집"""
        try:
            # 더 많은 테이블 정보 수집
            extended_context = schema_retriever.create_context_summary(query, max_tables=10)
            
            return {
                "success": True,
                "context": extended_context,
                "table_count": 10
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "context": "확장 컨텍스트를 가져올 수 없습니다."
            }
    
    async def _analyze_table_relationships(self, query: str, context: Dict) -> Dict[str, Any]:
        """테이블 관계 분석"""
        relationship_keywords = ["join", "연결", "관계", "별", "의", "간"]
        
        has_relationship_need = any(keyword in query.lower() for keyword in relationship_keywords)
        
        return {
            "relationship_complexity": "high" if has_relationship_need else "low",
            "join_required": has_relationship_need,
            "estimated_tables": self._estimate_table_count(query),
            "relationship_confidence": 0.8 if has_relationship_need else 0.6
        }
    
    async def _analyze_data_quality_concerns(self, query: str) -> Dict[str, Any]:
        """데이터 품질 우려사항 분석"""
        quality_keywords = ["null", "빈", "없는", "0", "중복"]
        
        has_quality_concerns = any(keyword in query.lower() for keyword in quality_keywords)
        
        return {
            "quality_check_needed": has_quality_concerns,
            "null_handling_required": "null" in query.lower() or "빈" in query,
            "duplicate_check_needed": "중복" in query,
            "data_validation_score": 0.7 if has_quality_concerns else 0.9
        }
    
    def _generate_deep_analysis_recommendation(self, basic_result: Dict, relationship_analysis: Dict) -> str:
        """심층 분석 기반 추천사항 생성"""
        recommendations = []
        
        if basic_result.get("has_uncertainty", False):
            recommendations.append("추가 탐색을 통한 불확실성 해결 필요")
        
        if relationship_analysis.get("join_required", False):
            recommendations.append("복잡한 JOIN 로직으로 인한 성능 최적화 고려")
        
        if relationship_analysis.get("relationship_complexity") == "high":
            recommendations.append("테이블 관계 검증을 위한 데이터 탐색 권장")
        
        return "; ".join(recommendations) if recommendations else "추가 조치 불필요"
    
    def _assess_query_complexity(self, query: str) -> str:
        """쿼리 복잡도 평가"""
        complexity_indicators = {
            "high": ["join", "union", "subquery", "group by", "having", "window"],
            "medium": ["where", "order by", "distinct", "aggregate"],
            "low": ["select", "from", "limit"]
        }
        
        query_lower = query.lower()
        
        for level, keywords in complexity_indicators.items():
            if any(keyword in query_lower for keyword in keywords):
                return level
        
        return "low"
    
    def _estimate_table_count(self, query: str) -> int:
        """쿼리에서 예상되는 테이블 수 추정"""
        table_indicators = ["테이블", "에서", "의", "별", "간", "관계", "join", "from"]
        
        count = 0
        for indicator in table_indicators:
            if indicator in query.lower():
                count += 1
        
        return min(max(count, 1), 5)  # 1~5 범위로 제한
    
    def _parse_json_response(self, response_content: str) -> Dict[str, Any]:
        """JSON 응답 파싱"""
        try:
            # 코드 블록 제거
            content = response_content.strip()
            
            if content.startswith("```json"):
                content = content[7:]
            if content.startswith("```"):
                content = content[3:]
            if content.endswith("```"):
                content = content[:-3]
            
            content = content.strip()
            
            return json.loads(content)
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing failed: {str(e)}")
            logger.error(f"Original response: {response_content}")
            
            # 파싱 실패시 기본 구조 반환
            return {
                "has_uncertainty": False,
                "uncertainties": [],
                "confidence": 0.0,
                "error": f"JSON 파싱 실패: {str(e)}",
                "raw_response": response_content
            }
    
    def _create_fallback_result(self, analysis_type: str, error_msg: str) -> Dict[str, Any]:
        """분석 실패시 대체 결과 생성"""
        return {
            "analysis_type": analysis_type,
            "has_uncertainty": True,  # 안전하게 불확실성 있다고 가정
            "uncertainties": [{
                "type": "general",
                "description": f"분석 중 오류 발생: {error_msg}",
                "confidence": 0.0
            }],
            "confidence": 0.0,
            "error": error_msg,
            "fallback": True
        }
    
    def _create_fallback_validation_result(self, error_msg: str) -> Dict[str, Any]:
        """검증 실패시 대체 결과 생성"""
        return {
            "analysis_type": "validation_review",
            "validation_passed": False,
            "issues": [{
                "type": "system",
                "severity": "critical",
                "description": f"검증 중 오류 발생: {error_msg}",
                "suggestion": "수동 검토 필요"
            }],
            "confidence": 0.0,
            "error": error_msg,
            "fallback": True
        }
    
    def get_agent_statistics(self) -> Dict[str, Any]:
        """Agent 통계 정보 반환"""
        if not self.analysis_history:
            return {"message": "분석 이력이 없습니다."}
        
        total_analyses = len(self.analysis_history)
        uncertainty_detected = sum(1 for h in self.analysis_history if h["has_uncertainty"])
        avg_confidence = sum(h["confidence"] for h in self.analysis_history) / total_analyses
        avg_processing_time = sum(h["processing_time"] for h in self.analysis_history) / total_analyses
        
        return {
            "total_analyses": total_analyses,
            "uncertainty_detection_rate": round((uncertainty_detected / total_analyses) * 100, 2),
            "average_confidence": round(avg_confidence, 3),
            "average_processing_time": round(avg_processing_time, 3),
            "performance_grade": "A" if avg_confidence > 0.8 and avg_processing_time < 3.0 else "B"
        }

# Agent 생성 헬퍼 함수
def create_schema_analyzer_agent(custom_config: Optional[Dict[str, Any]] = None) -> SchemaAnalyzerAgent:
    """SchemaAnalyzer Agent 생성"""
    config = create_agent_config(
        name="schema_analyzer",
        specialization="schema_analysis_uncertainty_detection",
        model="gpt-4",
        temperature=0.2,
        max_tokens=2000,
        **(custom_config or {})
    )
    
    return SchemaAnalyzerAgent(config)