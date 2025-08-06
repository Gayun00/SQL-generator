"""
UserCommunicator Agent - 사용자 커뮤니케이션 및 재질문 전문 Agent

간소화된 3단계 플로우로 구현:
1. generate_question: 질문 생성
2. wait_for_answer: 사용자 응답 대기  
3. finalize: 최종 결과 정리
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

@dataclass
class UserCommunicatorInput:
    """UserCommunicator 입력 데이터 구조"""
    step: Literal["generate_question", "wait_for_answer", "finalize"]
    source: Literal["user", "sql_generator", "data_explorer"]
    userInput: Optional[str] = None
    agentRequest: Optional[Dict[str, Any]] = None  # {"missingFields": [], "reason": ""}
    userReply: Optional[str] = None

@dataclass 
class UserCommunicatorOutput:
    """UserCommunicator 출력 데이터 구조"""
    step: Literal["wait_for_answer", "finalize"]
    questions: List[str]
    originalSource: Literal["user", "sql_generator", "data_explorer"]
    userReply: Optional[str] = None
    finalizedInput: Optional[str] = None
    nextAgentHint: Optional[str] = None

class UserCommunicatorAgent(SimpleBaseAgent):
    """사용자 커뮤니케이션 및 재질문 전문 Agent - 간소화된 버전"""
    
    def __init__(self, config: Optional[AgentConfig] = None):
        if config is None:
            config = create_agent_config(
                name="user_communicator",
                specialization="user_communication_clarification",
                model="gpt-4",
                temperature=0.3,
                max_tokens=1000
            )
        
        super().__init__(config)
        logger.info(f"UserCommunicator Agent initialized")
    
    def get_system_prompt(self) -> str:
        """사용자 커뮤니케이션 전문 시스템 프롬프트"""
        return """
        당신은 사용자 커뮤니케이션 전문 AI Agent입니다.
        
        **핵심 역할:**
        1. 사용자의 자연어 요청을 분석하여 모호한 부분 식별
        2. 다른 에이전트의 기술적 요청을 사용자 친화적 질문으로 변환
        3. 사용자 응답을 정리하여 다음 단계로 전달
        
        **질문 생성 원칙:**
        - 명확하고 이해하기 쉬운 언어 사용
        - 구체적인 선택지 제공
        - 한 번에 1-3개의 핵심 질문만 제시
        - 예시 포함하여 답변 용이성 극대화
        """
    
    async def process_message(self, message: AgentMessage) -> AgentMessage:
        """메시지 처리 - 3단계 플로우"""
        try:
            # 입력 데이터 파싱
            content = message.content
            input_data = UserCommunicatorInput(
                step=content.get("step"),
                source=content.get("source"), 
                userInput=content.get("userInput"),
                agentRequest=content.get("agentRequest"),
                userReply=content.get("userReply")
            )
            
            # 단계별 처리
            if input_data.step == "generate_question":
                result = await self._generate_question(input_data)
            elif input_data.step == "wait_for_answer":
                result = await self._wait_for_answer(input_data)
            elif input_data.step == "finalize":
                result = await self._finalize(input_data)
            else:
                raise ValueError(f"Unknown step: {input_data.step}")
            
            # 결과 반환
            return self.create_response_message(message, asdict(result))
            
        except Exception as e:
            logger.error(f"UserCommunicator processing failed: {str(e)}")
            return self.create_error_message(message, e)
    
    async def _generate_question(self, input_data: UserCommunicatorInput) -> UserCommunicatorOutput:
        """질문 생성 단계"""
        logger.info(f"Generating question for source: {input_data.source}")
        
        if input_data.source == "user":
            return await self._handle_user_input(input_data.userInput, input_data.source)
        elif input_data.source == "sql_generator":
            return await self._handle_agent_request(input_data.agentRequest, input_data.source)
        elif input_data.source == "data_explorer":
            return await self._handle_agent_request(input_data.agentRequest, input_data.source)
        else:
            raise ValueError(f"Unknown source: {input_data.source}")
    
    async def _wait_for_answer(self, input_data: UserCommunicatorInput) -> UserCommunicatorOutput:
        """사용자 응답 대기 단계 (실제 구현에서는 UI와 연동)"""
        return UserCommunicatorOutput(
            step="finalize",
            questions=[],
            originalSource=input_data.source,
            userReply=input_data.userReply
        )
    
    async def _finalize(self, input_data: UserCommunicatorInput) -> UserCommunicatorOutput:
        """최종 정리 단계"""
        logger.info("Finalizing user communication")
        
        # 사용자 응답을 바탕으로 최종 입력 정리
        finalized_input = await self._create_finalized_input(
            input_data.userReply,
            input_data.source
        )
        
        # 다음 에이전트 힌트 결정
        next_hint = self._determine_next_agent(input_data.source)
        
        return UserCommunicatorOutput(
            step="finalize",
            questions=[],
            originalSource=input_data.source,
            userReply=input_data.userReply,
            finalizedInput=finalized_input,
            nextAgentHint=next_hint
        )
    
    async def _handle_user_input(self, user_input: str, source: str) -> UserCommunicatorOutput:
        """사용자 초기 입력 처리"""
        # 1단계: 입력 유효성 검사
        if not self._is_valid_user_input(user_input):
            return UserCommunicatorOutput(
                step="wait_for_answer",
                questions=["유효한 질문을 입력해주세요"],
                originalSource=source
            )
        
        # 2단계: 모호성 분석
        system_prompt = f"""
        사용자의 자연어 요청을 분석하여 SQL 생성에 필요한 추가 정보가 있는지 판단하세요.
        
        사용자 요청: {user_input}
        
        다음 사항들을 확인해보세요:
        1. 날짜/기간 정보가 필요한가?
        2. 정렬 방식이 명시되었는가?  
        3. 필터링 조건이 명확한가?
        4. 집계 방식이 명확한가?
        
        추가 정보가 필요하면 사용자 친화적인 질문을 생성하세요.
        
        응답 형식 (JSON):
        {{
            "needs_clarification": true/false,
            "questions": ["질문1", "질문2", ...],
            "reason": "추가 정보가 필요한 이유"
        }}
        """
        
        try:
            response_content = await self.send_llm_request(system_prompt)
            parsed_response = self._parse_json_response(response_content)
            
            if parsed_response and parsed_response.get("needs_clarification", False):
                questions = parsed_response.get("questions", [])
            else:
                questions = []
            
            return UserCommunicatorOutput(
                step="wait_for_answer" if questions else "finalize",
                questions=questions,
                originalSource=source,
                finalizedInput=user_input if not questions else None
            )
            
        except Exception as e:
            logger.error(f"User input handling failed: {str(e)}")
            # 폴백: 기본 질문 생성
            return UserCommunicatorOutput(
                step="wait_for_answer",
                questions=["어떤 기간의 데이터를 조회하시겠어요?"],
                originalSource=source
            )
    
    async def _handle_agent_request(self, agent_request: Dict[str, Any], source: str) -> UserCommunicatorOutput:
        """다른 에이전트의 요청 처리"""
        missing_fields = agent_request.get("missingFields", [])
        reason = agent_request.get("reason", "")
        
        system_prompt = f"""
        다른 에이전트가 다음과 같은 정보를 요청했습니다:
        
        필요한 정보: {missing_fields}
        이유: {reason}
        
        이를 사용자가 이해하기 쉬운 질문으로 변환하세요.
        기술적 용어는 피하고 구체적인 예시나 선택지를 제공하세요.
        
        응답 형식 (JSON):
        {{
            "questions": ["사용자 친화적 질문1", "질문2", ...],
            "explanation": "왜 이 정보가 필요한지 간단한 설명"
        }}
        """
        
        try:
            response_content = await self.send_llm_request(system_prompt)
            parsed_response = self._parse_json_response(response_content)
            
            questions = parsed_response.get("questions", []) if parsed_response else []
            
            if not questions:
                # 폴백 질문 생성
                questions = [f"{field}에 대한 추가 정보가 필요합니다. 구체적으로 알려주세요." 
                           for field in missing_fields[:2]]
            
            return UserCommunicatorOutput(
                step="wait_for_answer",
                questions=questions,
                originalSource=source
            )
            
        except Exception as e:
            logger.error(f"Agent request handling failed: {str(e)}")
            return UserCommunicatorOutput(
                step="wait_for_answer",
                questions=["추가 정보가 필요합니다. 더 구체적으로 설명해 주세요."],
                originalSource=source
            )
    
    async def _create_finalized_input(self, user_reply: str, source: str) -> str:
        """사용자 응답을 바탕으로 최종 입력 생성"""
        if not user_reply:
            return ""
            
        system_prompt = f"""
        사용자의 응답을 분석하여 SQL 생성에 필요한 구조화된 정보를 JSON 형태로 추출하세요.
        
        사용자 응답: {user_reply}
        원본 소스: {source}
        
        다음 형식으로 응답하세요 (JSON):
        {{
            "query_type": "데이터 조회 유형 (예: aggregate, filter, join 등)",
            "time_range": {{
                "period": "기간 (예: 3개월, 1년 등)",
                "start_date": "시작일 (가능한 경우)",
                "end_date": "종료일 (가능한 경우)"
            }},
            "sort_criteria": {{
                "field": "정렬 기준 필드",
                "order": "asc 또는 desc"
            }},
            "filters": ["필터 조건들"],
            "aggregations": ["집계 조건들"],
            "tables_involved": ["관련 테이블들"],
            "description": "간단한 요약"
        }}
        
        정보가 없는 필드는 null로 설정하세요.
        """
        
        try:
            response = await self.send_llm_request(system_prompt)
            # JSON 파싱 검증
            parsed = self._parse_json_response(response)
            if parsed:
                return response.strip()
            else:
                # JSON 파싱 실패시 기본 구조 반환
                return self._create_fallback_finalized_input(user_reply)
        except Exception as e:
            logger.error(f"Finalized input creation failed: {str(e)}")
            return self._create_fallback_finalized_input(user_reply)
    
    def _determine_next_agent(self, source: str) -> str:
        """다음 에이전트 결정"""
        if source == "user":
            return "sql_generator"
        elif source == "sql_generator":
            return "sql_generator"  # 다시 SQL 생성으로
        elif source == "data_explorer":
            return "data_explorer"  # 다시 데이터 탐색으로
        else:
            return "orchestrator"
    
    def _is_valid_user_input(self, user_input: str) -> bool:
        """사용자 입력 유효성 검사"""
        if not user_input or not user_input.strip():
            return False
        
        # 너무 짧거나 무의미한 입력 체크
        cleaned_input = user_input.strip()
        if len(cleaned_input) < 2:
            return False
        
        # 특수 문자만 있거나 반복 문자 체크
        meaningless_patterns = ["ㅇㅇ", "ㅎㅎ", "ㅋㅋ", ".", "..", "...", "?", "??", "???"]
        if cleaned_input in meaningless_patterns:
            return False
        
        # 모든 문자가 같은 경우 (aaa, 111 등)
        if len(set(cleaned_input)) == 1 and len(cleaned_input) > 1:
            return False
        
        return True
    
    def _create_fallback_finalized_input(self, user_reply: str) -> str:
        """JSON 파싱 실패시 기본 구조화된 응답 생성"""
        return json.dumps({
            "query_type": "general",
            "time_range": {
                "period": None,
                "start_date": None,
                "end_date": None
            },
            "sort_criteria": {
                "field": None,
                "order": None
            },
            "filters": [],
            "aggregations": [],
            "tables_involved": [],
            "description": user_reply,
            "raw_input": user_reply
        }, ensure_ascii=False)
    
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
def create_user_communicator_agent(custom_config: Optional[Dict[str, Any]] = None) -> UserCommunicatorAgent:
    """UserCommunicator Agent 생성"""
    config = create_agent_config(
        name="user_communicator",
        specialization="user_communication_clarification",
        model="gpt-4",
        temperature=0.3,
        max_tokens=1000,
        **(custom_config or {})
    )
    
    return UserCommunicatorAgent(config)

