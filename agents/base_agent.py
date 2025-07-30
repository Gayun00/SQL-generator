"""
BaseAgent - A2A 시스템의 기본 Agent 추상 클래스

모든 전문 Agent들이 상속받는 기본 클래스로, 공통 인터페이스와 
기본 기능을 제공합니다.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage
import asyncio
import uuid
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MessageType(Enum):
    """Agent 간 메시지 타입"""
    REQUEST = "request"           # 작업 요청
    RESPONSE = "response"         # 작업 결과
    NOTIFICATION = "notification" # 상태 알림
    ERROR = "error"              # 오류 보고
    FEEDBACK = "feedback"        # 피드백 요청

class AgentStatus(Enum):
    """Agent 상태"""
    IDLE = "idle"                # 대기 중
    PROCESSING = "processing"    # 작업 중
    WAITING = "waiting"          # 응답 대기
    ERROR = "error"             # 오류 상태
    COMPLETED = "completed"      # 작업 완료

@dataclass
class AgentMessage:
    """Agent 간 통신을 위한 메시지 구조"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    sender: str = ""
    receiver: str = ""
    message_type: MessageType = MessageType.REQUEST
    content: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    correlation_id: Optional[str] = None  # 관련 메시지들을 그룹화
    priority: int = 5  # 1(높음) ~ 10(낮음)
    timeout: Optional[int] = None  # 타임아웃 (초)
    
    def to_dict(self) -> Dict[str, Any]:
        """메시지를 딕셔너리로 변환"""
        return {
            "id": self.id,
            "sender": self.sender,
            "receiver": self.receiver,
            "message_type": self.message_type.value,
            "content": self.content,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
            "correlation_id": self.correlation_id,
            "priority": self.priority,
            "timeout": self.timeout
        }

@dataclass
class AgentConfig:
    """Agent 설정"""
    name: str
    specialization: str
    model: str = "gpt-4"
    temperature: float = 0.3
    max_tokens: int = 2000
    timeout: int = 30
    max_retries: int = 3
    custom_prompt: Optional[str] = None
    
class BaseAgent(ABC):
    """모든 Agent의 기본 추상 클래스"""
    
    def __init__(self, config: AgentConfig):
        self.config = config
        self.name = config.name
        self.specialization = config.specialization
        self.status = AgentStatus.IDLE
        self.llm = self._initialize_llm()
        self.message_history: List[AgentMessage] = []
        self.performance_metrics = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "average_response_time": 0.0,
            "last_activity": None
        }
        
    
    def _initialize_llm(self) -> ChatOpenAI:
        """Agent별 최적화된 LLM 초기화"""
        return ChatOpenAI(
            model=self.config.model,
            temperature=self.config.temperature,
            max_tokens=self.config.max_tokens
        )
    
    @abstractmethod
    async def process_message(self, message: AgentMessage) -> AgentMessage:
        """
        메시지 처리 - 각 Agent가 구현해야 하는 핵심 메서드
        
        Args:
            message: 처리할 메시지
            
        Returns:
            AgentMessage: 처리 결과 메시지
        """
        pass
    
    @abstractmethod
    def get_system_prompt(self) -> str:
        """
        Agent별 전문화된 시스템 프롬프트 반환
        
        Returns:
            str: 시스템 프롬프트
        """
        pass
    
    async def send_llm_request(self, user_message: str, context: Optional[Dict] = None) -> str:
        """
        LLM에 요청을 보내고 응답을 받는 공통 메서드
        
        Args:
            user_message: 사용자 메시지
            context: 추가 컨텍스트 정보
            
        Returns:
            str: LLM 응답
        """
        try:
            # 시스템 프롬프트 구성
            system_prompt = self.get_system_prompt()
            if context:
                system_prompt += f"\n\n추가 컨텍스트:\n{context}"
            
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_message)
            ]
            
            # LLM 호출
            start_time = datetime.now()
            response = await self.llm.ainvoke(messages)
            end_time = datetime.now()
            
            # 성능 메트릭 업데이트
            response_time = (end_time - start_time).total_seconds()
            self._update_performance_metrics(True, response_time)
            
            logger.info(f"Agent '{self.name}' 요청 처리 완료 ({response_time:.2f}초)")
            return response.content
            
        except Exception as e:
            logger.error(f"Agent '{self.name}' LLM request 실패: {str(e)}")
            self._update_performance_metrics(False, 0)
            raise
    
    def _update_performance_metrics(self, success: bool, response_time: float):
        """성능 메트릭 업데이트"""
        self.performance_metrics["total_requests"] += 1
        
        if success:
            self.performance_metrics["successful_requests"] += 1
        else:
            self.performance_metrics["failed_requests"] += 1
        
        # 평균 응답 시간 계산
        total_successful = self.performance_metrics["successful_requests"]
        if total_successful > 0:
            current_avg = self.performance_metrics["average_response_time"]
            new_avg = ((current_avg * (total_successful - 1)) + response_time) / total_successful
            self.performance_metrics["average_response_time"] = new_avg
        
        self.performance_metrics["last_activity"] = datetime.now().isoformat()
    
    def get_status(self) -> Dict[str, Any]:
        """Agent 상태 정보 반환"""
        return {
            "name": self.name,
            "specialization": self.specialization,
            "status": self.status.value,
            "config": {
                "model": self.config.model,
                "temperature": self.config.temperature,
                "max_tokens": self.config.max_tokens
            },
            "performance": self.performance_metrics,
            "message_count": len(self.message_history)
        }
    
    def add_message_to_history(self, message: AgentMessage):
        """메시지 히스토리에 추가"""
        self.message_history.append(message)
        
        # 히스토리 크기 제한 (최근 100개만 유지)
        if len(self.message_history) > 100:
            self.message_history = self.message_history[-100:]
    
    async def validate_input(self, message: AgentMessage) -> bool:
        """
        입력 메시지 유효성 검증
        
        Args:
            message: 검증할 메시지
            
        Returns:
            bool: 유효한 경우 True
        """
        if not message.content:
            logger.warning(f"Agent '{self.name}' received empty message content")
            return False
        
        if message.message_type not in [MessageType.REQUEST, MessageType.FEEDBACK]:
            logger.warning(f"Agent '{self.name}' received unsupported message type: {message.message_type}")
            return False
        
        return True
    
    def create_response_message(self, original_message: AgentMessage, 
                              content: Dict[str, Any], 
                              message_type: MessageType = MessageType.RESPONSE) -> AgentMessage:
        """
        응답 메시지 생성
        
        Args:
            original_message: 원본 요청 메시지
            content: 응답 내용
            message_type: 메시지 타입
            
        Returns:
            AgentMessage: 생성된 응답 메시지
        """
        return AgentMessage(
            sender=self.name,
            receiver=original_message.sender,
            message_type=message_type,
            content=content,
            correlation_id=original_message.correlation_id or original_message.id,
            metadata={
                "response_to": original_message.id,
                "processing_time": self.performance_metrics.get("average_response_time", 0)
            }
        )
    
    def create_error_message(self, original_message: AgentMessage, 
                           error: Exception) -> AgentMessage:
        """
        오류 메시지 생성
        
        Args:
            original_message: 원본 요청 메시지
            error: 발생한 예외
            
        Returns:
            AgentMessage: 오류 메시지
        """
        return AgentMessage(
            sender=self.name,
            receiver=original_message.sender,
            message_type=MessageType.ERROR,
            content={
                "error_type": type(error).__name__,
                "error_message": str(error),
                "agent_status": self.status.value
            },
            correlation_id=original_message.correlation_id or original_message.id,
            metadata={
                "response_to": original_message.id,
                "error_timestamp": datetime.now().isoformat()
            }
        )
    
    async def cleanup(self):
        """Agent 정리 작업"""
        self.status = AgentStatus.IDLE
        logger.info(f"Agent '{self.name}' cleanup completed")

# Agent 팩토리 함수들
def create_agent_config(name: str, specialization: str, **kwargs) -> AgentConfig:
    """Agent 설정 생성 헬퍼 함수"""
    return AgentConfig(
        name=name,
        specialization=specialization,
        **kwargs
    )

# 성능 분석을 위한 유틸리티
class AgentPerformanceAnalyzer:
    """Agent 성능 분석기"""
    
    @staticmethod
    def analyze_agent_performance(agent: BaseAgent) -> Dict[str, Any]:
        """Agent 성능 분석"""
        metrics = agent.performance_metrics
        
        success_rate = 0
        if metrics["total_requests"] > 0:
            success_rate = (metrics["successful_requests"] / metrics["total_requests"]) * 100
        
        return {
            "agent_name": agent.name,
            "specialization": agent.specialization,
            "success_rate": round(success_rate, 2),
            "average_response_time": round(metrics["average_response_time"], 3),
            "total_requests": metrics["total_requests"],
            "current_status": agent.status.value,
            "efficiency_score": round((success_rate * 0.7) + ((1 / max(metrics["average_response_time"], 0.1)) * 30), 2)
        }
    
    @staticmethod
    def compare_agents(agents: List[BaseAgent]) -> Dict[str, Any]:
        """여러 Agent 성능 비교"""
        analyses = [AgentPerformanceAnalyzer.analyze_agent_performance(agent) for agent in agents]
        
        return {
            "total_agents": len(agents),
            "agent_performances": analyses,
            "best_performer": max(analyses, key=lambda x: x["efficiency_score"]) if analyses else None,
            "average_success_rate": sum(a["success_rate"] for a in analyses) / len(analyses) if analyses else 0
        }