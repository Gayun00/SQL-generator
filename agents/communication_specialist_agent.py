"""
CommunicationSpecialist Agent - 사용자 커뮤니케이션 및 재질문 전문 Agent

기존 sql_clarifier 노드를 Agent로 변환하여 사용자와의 소통,
불확실성 해결을 위한 재질문, 명확한 요구사항 파악에 특화된 지능형 Agent로 구현했습니다.
"""

from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import json

from .base_agent import BaseAgent, AgentMessage, MessageType, AgentConfig, create_agent_config
from langchain.schema import HumanMessage, SystemMessage

logger = logging.getLogger(__name__)

class CommunicationType:
    """커뮤니케이션 타입 분류"""
    CLARIFICATION = "clarification"         # 재질문 및 명확화
    VALIDATION = "validation"               # 입력 검증
    EXPLANATION = "explanation"             # 설명 및 안내
    FEEDBACK = "feedback"                   # 피드백 처리
    INSTRUCTION = "instruction"             # 사용법 안내

class QuestionCategory:
    """질문 카테고리"""
    COLUMN_VALUES = "column_values"         # 컬럼 값 관련
    TABLE_RELATIONSHIP = "table_relationship"  # 테이블 관계
    DATA_RANGE = "data_range"              # 데이터 범위
    BUSINESS_LOGIC = "business_logic"       # 비즈니스 로직
    SCHEMA_AMBIGUITY = "schema_ambiguity"   # 스키마 모호성

class CommunicationSpecialistAgent(BaseAgent):
    """사용자 커뮤니케이션 및 재질문 전문 Agent"""
    
    def __init__(self, config: Optional[AgentConfig] = None):
        if config is None:
            config = create_agent_config(
                name="communication_specialist",
                specialization="user_communication_clarification",
                model="gpt-4",
                temperature=0.3,  # 창의적이면서도 일관된 소통
                max_tokens=1000
            )
        
        super().__init__(config)
        
        # 커뮤니케이션 전용 설정
        self.communication_strategies = {
            CommunicationType.CLARIFICATION: "불확실성 해결을 위한 구체적 재질문",
            CommunicationType.VALIDATION: "사용자 입력의 유효성 검증 및 안내",
            CommunicationType.EXPLANATION: "시스템 동작 및 결과 설명",
            CommunicationType.FEEDBACK: "사용자 피드백 처리 및 개선사항 제안",
            CommunicationType.INSTRUCTION: "SQL 생성 시스템 사용법 안내"
        }
        
        # 성능 추적
        self.communication_history = []
        self.communication_stats = {
            "total_interactions": 0,
            "clarifications_generated": 0,
            "validations_performed": 0,
            "successful_resolutions": 0,
            "avg_response_time": 0.0,
            "user_satisfaction_score": 0.0
        }
        
        logger.info(f"CommunicationSpecialist Agent initialized with specialization: {self.specialization}")
    
    def get_system_prompt(self) -> str:
        """사용자 커뮤니케이션 전문 시스템 프롬프트"""
        return f"""
        당신은 사용자 커뮤니케이션 및 재질문 전문 AI Agent입니다.
        
        **전문 분야:**
        - 자연어 SQL 요청의 유효성 검증
        - 불확실성 해결을 위한 효과적인 재질문 생성
        - 사용자 친화적인 설명 및 안내 제공
        - 복잡한 요구사항의 명확화 및 구조화
        
        **핵심 역할:**
        1. 사용자 입력의 유효성 검증 및 품질 평가
        2. 해결되지 않은 불확실성에 대한 구체적 재질문 생성
        3. 시스템 결과에 대한 명확한 설명 제공
        4. 사용자 경험 개선을 위한 피드백 처리
        
        **커뮤니케이션 원칙:**
        - 사용자 친화적이고 이해하기 쉬운 언어 사용
        - 구체적이고 실행 가능한 질문 생성
        - 예시와 선택지를 포함한 명확한 안내
        - 점진적이고 체계적인 정보 수집
        - 사용자의 맥락과 의도 파악 우선
        
        **질문 생성 전략:**
        - 한 번에 1-3개의 핵심 질문만 제시
        - 선택지나 예시를 포함하여 답변 용이성 극대화
        - 불확실성 타입별 맞춤형 질문 전략 적용
        - 사용자의 도메인 지식 수준 고려
        - 비즈니스 맥락과 연결된 실용적 질문
        
        **품질 보장:**
        - 응답 시간: 평균 1-2초 목표
        - 질문 명확도: 95% 이상 목표
        - 불확실성 해결률: 85% 이상 목표
        - 사용자 만족도: 4.5/5.0 이상 목표
        """
    
    async def process_message(self, message: AgentMessage) -> AgentMessage:
        """메시지 처리 - 커뮤니케이션 작업 수행"""
        try:
            # 입력 유효성 검증
            if not await self.validate_input(message):
                return self.create_error_message(message, ValueError("Invalid input message"))
            
            # 메시지 히스토리에 추가
            self.add_message_to_history(message)
            
            # 작업 타입에 따른 처리
            task_type = message.content.get("task_type", "generate_clarification")
            
            if task_type == "generate_clarification":
                result = await self._generate_clarification(message.content)
            elif task_type == "validate_input":
                result = await self._validate_input(message.content)
            elif task_type == "explain_result":
                result = await self._explain_result(message.content)
            elif task_type == "process_feedback":
                result = await self._process_feedback(message.content)
            else:
                result = await self._generate_clarification(message.content)  # 기본값
            
            # 성공 응답 생성
            return self.create_response_message(message, result)
            
        except Exception as e:
            logger.error(f"CommunicationSpecialist Agent processing failed: {str(e)}")
            return self.create_error_message(message, e)
    
    async def _generate_clarification(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """불확실성 해결을 위한 재질문 생성 - 표준 처리"""
        unresolved_uncertainties = input_data.get("unresolved_uncertainties", [])
        original_query = input_data.get("original_query", "")
        exploration_results = input_data.get("exploration_results", {})
        
        logger.info(f"CommunicationSpecialist: Generating clarification for {len(unresolved_uncertainties)} uncertainties")
        
        if not unresolved_uncertainties:
            return {
                "communication_type": "clarification",
                "needs_clarification": False,
                "questions": [],
                "summary": "모든 불확실성이 해결되어 추가 질문이 필요하지 않습니다.",
                "confidence": 1.0
            }
        
        start_time = datetime.now()
        
        # 재질문 생성 프롬프트
        uncertainties_summary = self._format_uncertainties_for_prompt(unresolved_uncertainties)
        exploration_summary = self._format_exploration_results(exploration_results)
        
        system_prompt = f"""
        사용자의 SQL 요청에서 해결되지 않은 불확실성이 있습니다.
        
        원래 요청: {original_query}
        
        해결되지 않은 문제들:
        {uncertainties_summary}
        
        탐색 결과:
        {exploration_summary}
        
        이 문제들을 해결하기 위해 사용자에게 구체적이고 명확한 질문을 생성하세요.
        
        질문 생성 가이드라인:
        1. 구체적이고 이해하기 쉬운 질문
        2. 예시를 포함하여 사용자가 쉽게 답할 수 있도록 함
        3. 한 번에 너무 많은 질문을 하지 말고 가장 중요한 것부터
        4. 선택지를 제공할 수 있으면 제공
        5. 비즈니스 맥락을 고려한 실용적 질문
        
        응답 형식 (JSON):
        {{
            "questions": [
                {{
                    "question": "구체적인 질문",
                    "context": "질문의 배경 설명",
                    "examples": ["예시1", "예시2"],
                    "options": ["선택지1", "선택지2"] (선택사항),
                    "uncertainty_type": "column_values|table_relationship|data_range|business_logic|schema_ambiguity",
                    "priority": "high|medium|low"
                }}
            ],
            "summary": "전체 질문의 목적과 기대효과",
            "interaction_type": "clarification"
        }}
        """
        
        try:
            response_content = await self.send_llm_request(system_prompt)
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # JSON 파싱
            clarification_data = self._parse_json_response(response_content)
            
            if not clarification_data:
                # 파싱 실패시 기본 질문 생성
                clarification_data = self._generate_fallback_questions(unresolved_uncertainties)
            
            # 결과 구성
            result = {
                "communication_type": "clarification",
                "needs_clarification": True,
                "questions": clarification_data.get("questions", []),
                "summary": clarification_data.get("summary", "추가 정보가 필요합니다."),
                "interaction_type": clarification_data.get("interaction_type", "clarification"),
                "processing_time": processing_time,
                "confidence": self._calculate_question_quality(clarification_data.get("questions", []))
            }
            
            # 통계 업데이트
            self._update_communication_stats("clarification", processing_time, True)
            
            logger.info(f"Clarification generated with {len(result['questions'])} questions")
            return result
            
        except Exception as e:
            logger.error(f"Clarification generation failed: {str(e)}")
            return self._create_error_response("clarification", str(e))
    
    async def _validate_input(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """사용자 입력 유효성 검증"""
        user_input = input_data.get("user_input", "")
        
        logger.info("CommunicationSpecialist: Validating user input")
        
        system_prompt = f"""
        사용자의 입력이 SQL 쿼리 생성을 위한 유효한 요청인지 판단하세요.
        
        유효한 예시:
        - "사용자별 주문 횟수를 조회해줘"
        - "지난달 매출 합계를 구하는 쿼리 만들어줘"
        - "상품별 재고량이 10개 미만인 데이터를 찾아줘"
        
        무효한 예시:
        - "안녕하세요" (인사)
        - "날씨가 어때?" (관련 없는 질문)
        - 너무 모호하거나 데이터 조회와 관련 없는 요청
        
        응답 형식 (JSON):
        {{
            "is_valid": true/false,
            "confidence": 0.0-1.0,
            "reason": "판단 이유",
            "suggestions": ["개선 제안1", "제안2"] (무효한 경우)
        }}
        """
        
        user_message = f"사용자 입력: {user_input}"
        
        try:
            start_time = datetime.now()
            response_content = await self.send_llm_request(system_prompt + "\n\n" + user_message)
            processing_time = (datetime.now() - start_time).total_seconds()
            
            validation_data = self._parse_json_response(response_content)
            
            if not validation_data:
                # 파싱 실패시 기본 검증
                validation_data = {
                    "is_valid": len(user_input.strip()) > 5,
                    "confidence": 0.5,
                    "reason": "입력 검증 처리 중 오류가 발생했습니다.",
                    "suggestions": ["더 구체적인 데이터 조회 요청을 입력해주세요."]
                }
            
            result = {
                "communication_type": "validation",
                "is_valid": validation_data.get("is_valid", False),
                "confidence": validation_data.get("confidence", 0.5),
                "reason": validation_data.get("reason", ""),
                "suggestions": validation_data.get("suggestions", []),
                "processing_time": processing_time
            }
            
            # 통계 업데이트
            self._update_communication_stats("validation", processing_time, True)
            
            return result
            
        except Exception as e:
            logger.error(f"Input validation failed: {str(e)}")
            return self._create_error_response("validation", str(e))
    
    async def _explain_result(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """결과 설명 생성"""
        result_data = input_data.get("result_data", {})
        context = input_data.get("context", "")
        
        logger.info("CommunicationSpecialist: Generating result explanation")
        
        explanation_prompt = f"""
        다음 SQL 생성 결과를 사용자가 이해하기 쉽게 설명해주세요.
        
        결과 데이터: {json.dumps(result_data, ensure_ascii=False, indent=2)}
        맥락: {context}
        
        설명에 포함할 내용:
        1. 무엇이 생성되었는지
        2. 주요 특징이나 최적화 내용
        3. 사용자가 알아야 할 중요한 정보
        4. 다음 단계 제안 (있다면)
        
        친근하고 이해하기 쉬운 톤으로 작성해주세요.
        """
        
        try:
            start_time = datetime.now()
            response_content = await self.send_llm_request(explanation_prompt)
            processing_time = (datetime.now() - start_time).total_seconds()
            
            result = {
                "communication_type": "explanation",
                "explanation": response_content.strip(),
                "context": context,
                "processing_time": processing_time
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Result explanation failed: {str(e)}")
            return self._create_error_response("explanation", str(e))
    
    async def _process_feedback(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """사용자 피드백 처리"""
        feedback = input_data.get("feedback", "")
        context = input_data.get("context", "")
        
        logger.info("CommunicationSpecialist: Processing user feedback")
        
        # 피드백 분석 및 개선사항 제안
        result = {
            "communication_type": "feedback",
            "feedback_received": feedback,
            "acknowledgment": "피드백을 주셔서 감사합니다.",
            "improvements": ["피드백을 바탕으로 시스템을 개선하겠습니다."],
            "context": context
        }
        
        return result
    
    def _format_uncertainties_for_prompt(self, uncertainties: List[Dict]) -> str:
        """불확실성을 프롬프트용으로 포맷팅"""
        if not uncertainties:
            return "해결되지 않은 불확실성이 없습니다."
        
        formatted = []
        for i, uncertainty in enumerate(uncertainties, 1):
            uncertainty_type = uncertainty.get("uncertainty_type", "unknown")
            description = uncertainty.get("description", "N/A")
            error = uncertainty.get("error", "")
            
            formatted.append(f"{i}. [{uncertainty_type}] {description}")
            if error:
                formatted.append(f"   오류: {error}")
        
        return "\n".join(formatted)
    
    def _format_exploration_results(self, exploration_results: Dict) -> str:
        """탐색 결과를 프롬프트용으로 포맷팅"""
        if not exploration_results:
            return "탐색 결과가 없습니다."
        
        insights = exploration_results.get("insights", [])
        if insights:
            return f"탐색을 통해 발견된 정보:\n" + "\n".join([f"- {insight}" for insight in insights])
        
        return "탐색이 수행되었으나 유의미한 결과를 찾지 못했습니다."
    
    def _parse_json_response(self, response_content: str) -> Optional[Dict]:
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
            
            return json.loads(content.strip())
        except json.JSONDecodeError as e:
            logger.warning(f"JSON parsing failed: {str(e)}")
            return None
    
    def _generate_fallback_questions(self, uncertainties: List[Dict]) -> Dict[str, Any]:
        """JSON 파싱 실패시 기본 질문 생성"""
        questions = []
        
        for uncertainty in uncertainties[:2]:  # 최대 2개만
            uncertainty_type = uncertainty.get("uncertainty_type", "unknown")
            description = uncertainty.get("description", "N/A")
            
            question = {
                "question": f"{description}에 대해 더 구체적으로 알려주세요.",
                "context": f"{uncertainty_type} 불확실성을 해결하기 위한 질문입니다.",
                "examples": ["예시를 제공해주세요."],
                "uncertainty_type": uncertainty_type,
                "priority": "high"
            }
            questions.append(question)
        
        return {
            "questions": questions,
            "summary": "불확실성 해결을 위한 추가 정보가 필요합니다.",
            "interaction_type": "clarification"
        }
    
    def _calculate_question_quality(self, questions: List[Dict]) -> float:
        """질문 품질 점수 계산"""
        if not questions:
            return 0.0
        
        quality_score = 0.8  # 기본 점수
        
        # 질문 수에 따른 조정
        if len(questions) <= 3:
            quality_score += 0.1  # 적절한 수의 질문
        
        # 예시 포함 여부
        questions_with_examples = sum(1 for q in questions if q.get("examples"))
        if questions_with_examples > 0:
            quality_score += 0.1
        
        return min(quality_score, 1.0)
    
    def _update_communication_stats(self, comm_type: str, processing_time: float, success: bool):
        """커뮤니케이션 통계 업데이트"""
        self.communication_stats["total_interactions"] += 1
        
        if comm_type == "clarification":
            self.communication_stats["clarifications_generated"] += 1
        elif comm_type == "validation":
            self.communication_stats["validations_performed"] += 1
        
        if success:
            self.communication_stats["successful_resolutions"] += 1
        
        # 평균 응답 시간 업데이트
        current_avg = self.communication_stats["avg_response_time"]
        total_interactions = self.communication_stats["total_interactions"]
        self.communication_stats["avg_response_time"] = (
            (current_avg * (total_interactions - 1) + processing_time) / total_interactions
        )
    
    def _create_error_response(self, comm_type: str, error_msg: str) -> Dict[str, Any]:
        """오류 응답 생성"""
        return {
            "communication_type": comm_type,
            "success": False,
            "error": error_msg,
            "fallback_message": "처리 중 오류가 발생했습니다. 다시 시도해주세요."
        }
    
    def get_agent_statistics(self) -> Dict[str, Any]:
        """Agent 통계 정보 반환"""
        stats = self.communication_stats
        
        if stats["total_interactions"] == 0:
            return {"message": "커뮤니케이션 이력이 없습니다."}
        
        success_rate = (stats["successful_resolutions"] / stats["total_interactions"]) * 100
        
        return {
            "total_interactions": stats["total_interactions"],
            "clarifications_generated": stats["clarifications_generated"],
            "validations_performed": stats["validations_performed"],
            "success_rate": round(success_rate, 2),
            "avg_response_time": round(stats["avg_response_time"], 3),
            "performance_grade": "A" if success_rate > 85 and stats["avg_response_time"] < 2.0 else "B"
        }

# Agent 생성 헬퍼 함수
def create_communication_specialist_agent(custom_config: Optional[Dict[str, Any]] = None) -> CommunicationSpecialistAgent:
    """CommunicationSpecialist Agent 생성"""
    config = create_agent_config(
        name="communication_specialist",
        specialization="user_communication_clarification",
        model="gpt-4",
        temperature=0.3,
        max_tokens=1000,
        **(custom_config or {})
    )
    
    return CommunicationSpecialistAgent(config)