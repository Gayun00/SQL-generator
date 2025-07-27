"""
Communication Hub - A2A 시스템의 메시지 라우팅 및 통신 관리

Agent 간 메시지 전달, 통신 모니터링, 오류 처리 및 로깅을 담당합니다.
중앙집중식 통신 허브로 모든 Agent 간 통신을 관리하고 최적화합니다.
"""

from typing import Dict, List, Optional, Any, Callable, Set
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import logging
from datetime import datetime, timedelta
from collections import defaultdict, deque
import json
import threading
from concurrent.futures import ThreadPoolExecutor

from .base_agent import AgentMessage, MessageType, BaseAgent

logger = logging.getLogger(__name__)

class RoutingStrategy(Enum):
    """메시지 라우팅 전략"""
    DIRECT = "direct"              # 직접 전달
    BROADCAST = "broadcast"        # 브로드캐스트
    ROUND_ROBIN = "round_robin"    # 라운드 로빈
    LOAD_BALANCED = "load_balanced" # 부하 분산

class MessagePriority(Enum):
    """메시지 우선순위"""
    URGENT = 1      # 긴급 (시스템 오류 등)
    HIGH = 2        # 높음 (핵심 작업)
    NORMAL = 3      # 보통
    LOW = 4         # 낮음 (로깅, 모니터링 등)

@dataclass
class RoutingRule:
    """라우팅 규칙"""
    sender_pattern: str = "*"          # 발신자 패턴
    receiver_pattern: str = "*"        # 수신자 패턴
    message_type: Optional[MessageType] = None
    strategy: RoutingStrategy = RoutingStrategy.DIRECT
    middleware: List[str] = field(default_factory=list)  # 미들웨어 체인
    timeout: int = 30
    retry_count: int = 3

@dataclass
class MessageQueue:
    """메시지 큐"""
    messages: deque = field(default_factory=deque)
    max_size: int = 1000
    priority_index: Dict[int, deque] = field(default_factory=lambda: defaultdict(deque))
    
    def enqueue(self, message: AgentMessage, priority: int = 3):
        """메시지 큐에 추가"""
        if len(self.messages) >= self.max_size:
            # 오래된 메시지 제거
            old_message = self.messages.popleft()
            old_priority = old_message.priority
            if old_priority in self.priority_index:
                try:
                    self.priority_index[old_priority].remove(old_message)
                except ValueError:
                    pass
        
        self.messages.append(message)
        self.priority_index[priority].append(message)
    
    def dequeue_by_priority(self) -> Optional[AgentMessage]:
        """우선순위에 따라 메시지 추출"""
        for priority in sorted(self.priority_index.keys()):
            if self.priority_index[priority]:
                message = self.priority_index[priority].popleft()
                self.messages.remove(message)
                return message
        return None

@dataclass
class CommunicationStats:
    """통신 통계"""
    total_messages: int = 0
    successful_deliveries: int = 0
    failed_deliveries: int = 0
    average_latency: float = 0.0
    message_types_count: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    agent_communication_matrix: Dict[str, Dict[str, int]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(int))
    )
    
    def update_delivery(self, success: bool, latency: float, message_type: str, 
                       sender: str, receiver: str):
        """배송 통계 업데이트"""
        self.total_messages += 1
        if success:
            self.successful_deliveries += 1
        else:
            self.failed_deliveries += 1
        
        # 평균 지연 시간 계산
        if self.successful_deliveries > 0:
            self.average_latency = (
                (self.average_latency * (self.successful_deliveries - 1) + latency) / 
                self.successful_deliveries
            )
        
        self.message_types_count[message_type] += 1
        self.agent_communication_matrix[sender][receiver] += 1

class MessageMiddleware:
    """메시지 미들웨어 기본 클래스"""
    
    async def process_message(self, message: AgentMessage, context: Dict[str, Any]) -> AgentMessage:
        """메시지 처리 - 상속받아 구현"""
        return message

class LoggingMiddleware(MessageMiddleware):
    """로깅 미들웨어"""
    
    async def process_message(self, message: AgentMessage, context: Dict[str, Any]) -> AgentMessage:
        logger.info(f"Message: {message.sender} → {message.receiver} [{message.message_type.value}]")
        return message

class ValidationMiddleware(MessageMiddleware):
    """메시지 유효성 검증 미들웨어"""
    
    async def process_message(self, message: AgentMessage, context: Dict[str, Any]) -> AgentMessage:
        if not message.sender or not message.receiver:
            raise ValueError("Message must have valid sender and receiver")
        
        if not message.content:
            logger.warning(f"Empty message content from {message.sender} to {message.receiver}")
        
        return message

class EncryptionMiddleware(MessageMiddleware):
    """암호화 미들웨어 (예시)"""
    
    async def process_message(self, message: AgentMessage, context: Dict[str, Any]) -> AgentMessage:
        # 실제 구현에서는 메시지 내용을 암호화
        message.metadata["encrypted"] = True
        return message

class CommunicationHub:
    """중앙집중식 통신 허브"""
    
    def __init__(self, max_concurrent_messages: int = 100):
        self.agents: Dict[str, BaseAgent] = {}
        self.message_queues: Dict[str, MessageQueue] = defaultdict(MessageQueue)
        self.routing_rules: List[RoutingRule] = []
        self.middleware: Dict[str, MessageMiddleware] = {
            "logging": LoggingMiddleware(),
            "validation": ValidationMiddleware(),
            "encryption": EncryptionMiddleware()
        }
        
        self.stats = CommunicationStats()
        self.max_concurrent_messages = max_concurrent_messages
        self.active_messages: Set[str] = set()
        self.message_history: deque = deque(maxlen=10000)
        
        # 비동기 처리용
        self._running = False
        self._processor_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        
        logger.info("CommunicationHub initialized")
    
    def register_agent(self, agent: BaseAgent):
        """Agent 등록"""
        self.agents[agent.name] = agent
        logger.info(f"Agent '{agent.name}' registered with communication hub")
    
    def unregister_agent(self, agent_name: str):
        """Agent 등록 해제"""
        if agent_name in self.agents:
            del self.agents[agent_name]
            # 해당 Agent의 큐 정리
            if agent_name in self.message_queues:
                del self.message_queues[agent_name]
            logger.info(f"Agent '{agent_name}' unregistered from communication hub")
    
    def add_routing_rule(self, rule: RoutingRule):
        """라우팅 규칙 추가"""
        self.routing_rules.append(rule)
        logger.info(f"Routing rule added: {rule.sender_pattern} → {rule.receiver_pattern}")
    
    def add_middleware(self, name: str, middleware: MessageMiddleware):
        """미들웨어 추가"""
        self.middleware[name] = middleware
        logger.info(f"Middleware '{name}' added")
    
    async def send_message(self, message: AgentMessage) -> bool:
        """
        메시지 전송
        
        Args:
            message: 전송할 메시지
            
        Returns:
            bool: 전송 성공 여부
        """
        start_time = datetime.now()
        
        try:
            # 동시 메시지 수 제한
            if len(self.active_messages) >= self.max_concurrent_messages:
                logger.warning("Maximum concurrent messages reached, queuing message")
                await asyncio.sleep(0.1)  # 잠시 대기
            
            self.active_messages.add(message.id)
            
            # 라우팅 규칙 적용
            routing_rule = self._find_routing_rule(message)
            
            # 미들웨어 체인 실행
            processed_message = await self._apply_middleware(message, routing_rule)
            
            # 메시지 전송
            success = await self._deliver_message(processed_message, routing_rule)
            
            # 통계 업데이트
            latency = (datetime.now() - start_time).total_seconds()
            self.stats.update_delivery(
                success, latency, processed_message.message_type.value,
                processed_message.sender, processed_message.receiver
            )
            
            # 히스토리에 추가
            self.message_history.append({
                "message_id": processed_message.id,
                "sender": processed_message.sender,
                "receiver": processed_message.receiver,
                "type": processed_message.message_type.value,
                "timestamp": start_time.isoformat(),
                "success": success,
                "latency": latency
            })
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to send message: {str(e)}")
            return False
        finally:
            self.active_messages.discard(message.id)
    
    def _find_routing_rule(self, message: AgentMessage) -> RoutingRule:
        """메시지에 적용할 라우팅 규칙 찾기"""
        for rule in self.routing_rules:
            if self._match_pattern(message.sender, rule.sender_pattern) and \
               self._match_pattern(message.receiver, rule.receiver_pattern):
                if rule.message_type is None or rule.message_type == message.message_type:
                    return rule
        
        # 기본 규칙 반환
        return RoutingRule()
    
    def _match_pattern(self, value: str, pattern: str) -> bool:
        """패턴 매칭"""
        if pattern == "*":
            return True
        return value == pattern
    
    async def _apply_middleware(self, message: AgentMessage, rule: RoutingRule) -> AgentMessage:
        """미들웨어 체인 적용"""
        processed_message = message
        context = {"routing_rule": rule}
        
        middleware_chain = rule.middleware if rule.middleware else ["validation", "logging"]
        
        for middleware_name in middleware_chain:
            if middleware_name in self.middleware:
                processed_message = await self.middleware[middleware_name].process_message(
                    processed_message, context
                )
        
        return processed_message
    
    async def _deliver_message(self, message: AgentMessage, rule: RoutingRule) -> bool:
        """메시지 배송"""
        if rule.strategy == RoutingStrategy.DIRECT:
            return await self._deliver_direct(message)
        elif rule.strategy == RoutingStrategy.BROADCAST:
            return await self._deliver_broadcast(message)
        elif rule.strategy == RoutingStrategy.ROUND_ROBIN:
            return await self._deliver_round_robin(message)
        elif rule.strategy == RoutingStrategy.LOAD_BALANCED:
            return await self._deliver_load_balanced(message)
        else:
            return await self._deliver_direct(message)
    
    async def _deliver_direct(self, message: AgentMessage) -> bool:
        """직접 배송"""
        receiver_agent = self.agents.get(message.receiver)
        if not receiver_agent:
            logger.error(f"Receiver agent '{message.receiver}' not found")
            return False
        
        try:
            # 메시지 큐에 추가
            queue = self.message_queues[message.receiver]
            queue.enqueue(message, message.priority)
            
            # Agent에게 알림 (실제 구현에서는 이벤트 기반으로 처리)
            logger.debug(f"Message queued for agent '{message.receiver}'")
            return True
            
        except Exception as e:
            logger.error(f"Failed to deliver message to '{message.receiver}': {str(e)}")
            return False
    
    async def _deliver_broadcast(self, message: AgentMessage) -> bool:
        """브로드캐스트 배송"""
        success_count = 0
        
        for agent_name, agent in self.agents.items():
            if agent_name != message.sender:  # 발신자 제외
                broadcast_message = AgentMessage(
                    sender=message.sender,
                    receiver=agent_name,
                    message_type=message.message_type,
                    content=message.content,
                    correlation_id=message.correlation_id
                )
                
                if await self._deliver_direct(broadcast_message):
                    success_count += 1
        
        return success_count > 0
    
    async def _deliver_round_robin(self, message: AgentMessage) -> bool:
        """라운드 로빈 배송 (로드 밸런싱용)"""
        # 간단한 구현 - 실제로는 더 정교한 로직 필요
        available_agents = [name for name in self.agents.keys() if name != message.sender]
        if not available_agents:
            return False
        
        # 간단한 라운드 로빈 선택
        selected_agent = available_agents[hash(message.id) % len(available_agents)]
        message.receiver = selected_agent
        
        return await self._deliver_direct(message)
    
    async def _deliver_load_balanced(self, message: AgentMessage) -> bool:
        """부하 분산 배송"""
        # Agent별 큐 길이를 기준으로 최적 Agent 선택
        min_queue_size = float('inf')
        best_agent = None
        
        for agent_name in self.agents.keys():
            if agent_name != message.sender:
                queue_size = len(self.message_queues[agent_name].messages)
                if queue_size < min_queue_size:
                    min_queue_size = queue_size
                    best_agent = agent_name
        
        if best_agent:
            message.receiver = best_agent
            return await self._deliver_direct(message)
        
        return False
    
    async def get_message_for_agent(self, agent_name: str) -> Optional[AgentMessage]:
        """Agent용 메시지 수신"""
        async with self._lock:
            queue = self.message_queues[agent_name]
            return queue.dequeue_by_priority()
    
    async def start_processing(self):
        """메시지 처리 시작"""
        if self._running:
            return
        
        self._running = True
        self._processor_task = asyncio.create_task(self._message_processor())
        logger.info("Communication hub message processing started")
    
    async def stop_processing(self):
        """메시지 처리 중지"""
        self._running = False
        
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Communication hub message processing stopped")
    
    async def _message_processor(self):
        """백그라운드 메시지 처리"""
        while self._running:
            try:
                # 각 Agent의 큐에서 메시지 처리
                for agent_name, agent in self.agents.items():
                    message = await self.get_message_for_agent(agent_name)
                    if message:
                        # Agent에게 메시지 전달 (실제 구현에서는 더 정교한 방식)
                        asyncio.create_task(self._process_agent_message(agent, message))
                
                await asyncio.sleep(0.1)  # CPU 사용률 조절
                
            except Exception as e:
                logger.error(f"Error in message processor: {str(e)}")
                await asyncio.sleep(1)
    
    async def _process_agent_message(self, agent: BaseAgent, message: AgentMessage):
        """Agent 메시지 처리"""
        try:
            response = await agent.process_message(message)
            if response:
                # 응답이 있으면 다시 라우팅
                await self.send_message(response)
        except Exception as e:
            logger.error(f"Agent message processing failed: {str(e)}")
    
    def get_communication_stats(self) -> Dict[str, Any]:
        """통신 통계 조회"""
        success_rate = 0
        if self.stats.total_messages > 0:
            success_rate = (self.stats.successful_deliveries / self.stats.total_messages) * 100
        
        return {
            "total_messages": self.stats.total_messages,
            "success_rate": round(success_rate, 2),
            "average_latency": round(self.stats.average_latency, 4),
            "message_types": dict(self.stats.message_types_count),
            "communication_matrix": {
                sender: dict(receivers) 
                for sender, receivers in self.stats.agent_communication_matrix.items()
            },
            "queue_status": {
                agent_name: len(queue.messages) 
                for agent_name, queue in self.message_queues.items()
            },
            "active_messages": len(self.active_messages)
        }
    
    def get_message_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """메시지 히스토리 조회"""
        return list(self.message_history)[-limit:]
    
    async def cleanup(self):
        """정리 작업"""
        await self.stop_processing()
        
        self.agents.clear()
        self.message_queues.clear()
        self.active_messages.clear()
        
        logger.info("CommunicationHub cleanup completed")

# 유틸리티 함수들
def create_default_hub() -> CommunicationHub:
    """기본 설정의 CommunicationHub 생성"""
    hub = CommunicationHub()
    
    # 기본 라우팅 규칙 추가
    hub.add_routing_rule(RoutingRule(
        sender_pattern="*",
        receiver_pattern="*",
        strategy=RoutingStrategy.DIRECT,
        middleware=["validation", "logging"]
    ))
    
    return hub

def create_high_performance_hub() -> CommunicationHub:
    """고성능 CommunicationHub 생성"""
    hub = CommunicationHub(max_concurrent_messages=500)
    
    # 성능 최적화된 라우팅 규칙
    hub.add_routing_rule(RoutingRule(
        sender_pattern="*",
        receiver_pattern="*", 
        strategy=RoutingStrategy.DIRECT,
        middleware=["validation"]  # 로깅 제거로 성능 향상
    ))
    
    return hub