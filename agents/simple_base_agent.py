"""
SimpleBaseAgent - UserCommunicatorAgent용 간소화된 기본 클래스
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import uuid
import logging
import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage

# .env 파일 로드
load_dotenv()

logger = logging.getLogger(__name__)

class MessageType(Enum):
    """메시지 타입"""
    TASK = "task"
    RESPONSE = "response" 
    ERROR = "error"

@dataclass
class AgentMessage:
    """Agent 간 통신 메시지"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    type: MessageType = MessageType.TASK
    source: str = ""
    target: str = ""
    content: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

@dataclass
class AgentConfig:
    """Agent 설정"""
    name: str
    specialization: str
    model: str = "gpt-4"
    temperature: float = 0.3
    max_tokens: int = 1000

class SimpleBaseAgent(ABC):
    """간소화된 기본 Agent 클래스"""
    
    def __init__(self, config: AgentConfig):
        self.config = config
        self.name = config.name
        self.specialization = config.specialization
        self.llm = self._initialize_llm()
    
    def _initialize_llm(self) -> ChatOpenAI:
        """LLM 초기화"""
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is not set")
        
        return ChatOpenAI(
            api_key=api_key,
            model=self.config.model,
            temperature=self.config.temperature,
            max_tokens=self.config.max_tokens
        )
    
    @abstractmethod
    async def process_message(self, message: AgentMessage) -> AgentMessage:
        """메시지 처리 - 각 Agent가 구현"""
        pass
    
    @abstractmethod
    def get_system_prompt(self) -> str:
        """시스템 프롬프트 반환"""
        pass
    
    async def send_llm_request(self, prompt: str) -> str:
        """LLM 요청"""
        try:
            # 시스템 프롬프트와 사용자 메시지 구성
            system_prompt = self.get_system_prompt()
            
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=prompt)
            ]
            
            # LLM 호출
            logger.info(f"LLM Request from {self.name}: {prompt[:100]}...")
            response = await self.llm.ainvoke(messages)
            
            return response.content
            
        except Exception as e:
            logger.error(f"LLM request failed: {str(e)}")
            raise
    
    def create_response_message(self, original_message: AgentMessage, content: Dict[str, Any]) -> AgentMessage:
        """응답 메시지 생성"""
        return AgentMessage(
            type=MessageType.RESPONSE,
            source=self.name,
            target=original_message.source,
            content=content
        )
    
    def create_error_message(self, original_message: AgentMessage, error: Exception) -> AgentMessage:
        """오류 메시지 생성"""
        return AgentMessage(
            type=MessageType.ERROR,
            source=self.name,
            target=original_message.source,
            content={
                "error_type": type(error).__name__,
                "error_message": str(error)
            }
        )

def create_agent_config(name: str, specialization: str, **kwargs) -> AgentConfig:
    """Agent 설정 생성 헬퍼"""
    return AgentConfig(
        name=name,
        specialization=specialization,
        **kwargs
    )