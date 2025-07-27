"""
MasterOrchestrator - A2A 시스템의 중앙 제어 및 조정 시스템

모든 Agent들을 관리하고, 작업 흐름을 조정하며, 최적의 실행 계획을 수립합니다.
중앙집중식 접근방식으로 안정성과 예측 가능성을 보장합니다.
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import logging
from datetime import datetime, timedelta
import json

from .base_agent import BaseAgent, AgentMessage, MessageType, AgentStatus, AgentConfig
from workflow.state import SQLGeneratorState

logger = logging.getLogger(__name__)

class ExecutionMode(Enum):
    """실행 모드"""
    SEQUENTIAL = "sequential"     # 순차 실행
    PARALLEL = "parallel"        # 병렬 실행
    HYBRID = "hybrid"            # 하이브리드 (상황에 따라)
    ADAPTIVE = "adaptive"        # 적응형 (실시간 조정)

class TaskPriority(Enum):
    """작업 우선순위"""
    CRITICAL = 1    # 긴급 (오류 복구 등)
    HIGH = 2        # 높음 (핵심 분석)
    MEDIUM = 3      # 보통 (일반 처리)
    LOW = 4         # 낮음 (최적화 등)

@dataclass
class ExecutionPlan:
    """실행 계획"""
    id: str
    phases: List['ExecutionPhase']
    mode: ExecutionMode
    estimated_duration: float  # 예상 소요 시간 (초)
    success_criteria: Dict[str, Any]
    fallback_plan: Optional['ExecutionPlan'] = None
    created_at: datetime = field(default_factory=datetime.now)

@dataclass
class ExecutionPhase:
    """실행 단계"""
    name: str
    agent_tasks: List['AgentTask']
    dependencies: List[str] = field(default_factory=list)  # 의존하는 단계들
    timeout: int = 30
    retry_count: int = 0
    max_retries: int = 2

@dataclass
class AgentTask:
    """Agent 작업"""
    agent_name: str
    task_type: str
    input_data: Dict[str, Any]
    priority: TaskPriority = TaskPriority.MEDIUM
    timeout: int = 30
    depends_on: List[str] = field(default_factory=list)  # 의존하는 다른 작업들
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ExecutionContext:
    """실행 컨텍스트"""
    query: str
    state: SQLGeneratorState
    complexity_score: float = 0.0
    user_context: Dict[str, Any] = field(default_factory=dict)
    execution_history: List[str] = field(default_factory=list)
    performance_requirements: Dict[str, Any] = field(default_factory=dict)

class QueryComplexityAnalyzer:
    """쿼리 복잡도 분석기"""
    
    @staticmethod
    def analyze_complexity(query: str, state: SQLGeneratorState) -> float:
        """
        쿼리 복잡도 분석 (0.0 ~ 1.0)
        
        Args:
            query: 사용자 쿼리
            state: 현재 상태
            
        Returns:
            float: 복잡도 점수
        """
        complexity_score = 0.0
        
        # 키워드 기반 복잡도 측정
        complex_keywords = ['join', 'union', 'subquery', '서브쿼리', '조인', '합계', '평균', '그룹']
        for keyword in complex_keywords:
            if keyword.lower() in query.lower():
                complexity_score += 0.1
        
        # 테이블 수 추정
        table_indicators = ['테이블', '에서', '의', '별', '간', '관계']
        table_count = sum(1 for indicator in table_indicators if indicator in query)
        complexity_score += min(table_count * 0.05, 0.3)
        
        # 상태 기반 복잡도
        if state.get("hasUncertainty"):
            complexity_score += 0.2
        
        if state.get("explorationResults"):
            complexity_score += 0.1
        
        return min(complexity_score, 1.0)

class MasterOrchestrator:
    """중앙 관리 방식의 Agent 조정자"""
    
    def __init__(self):
        self.agents: Dict[str, BaseAgent] = {}
        self.active_executions: Dict[str, ExecutionPlan] = {}
        self.execution_history: List[Dict[str, Any]] = []
        self.performance_stats = {
            "total_executions": 0,
            "successful_executions": 0,
            "average_execution_time": 0.0,
            "agent_utilization": {}
        }
        
        logger.info("MasterOrchestrator initialized")
    
    def register_agent(self, agent: BaseAgent):
        """Agent 등록"""
        self.agents[agent.name] = agent
        self.performance_stats["agent_utilization"][agent.name] = {
            "total_tasks": 0,
            "successful_tasks": 0,
            "average_response_time": 0.0
        }
        logger.info(f"Agent '{agent.name}' registered with orchestrator")
    
    def unregister_agent(self, agent_name: str):
        """Agent 등록 해제"""
        if agent_name in self.agents:
            del self.agents[agent_name]
            logger.info(f"Agent '{agent_name}' unregistered from orchestrator")
    
    async def process_sql_request(self, context: ExecutionContext) -> Dict[str, Any]:
        """
        SQL 요청 처리 - 메인 엔트리 포인트
        
        Args:
            context: 실행 컨텍스트
            
        Returns:
            Dict: 처리 결과
        """
        logger.info(f"Processing SQL request: '{context.query[:50]}...'")
        
        try:
            # 1. 복잡도 분석
            context.complexity_score = QueryComplexityAnalyzer.analyze_complexity(
                context.query, context.state
            )
            
            # 2. 실행 계획 수립
            execution_plan = await self._create_execution_plan(context)
            
            # 3. 계획 실행
            result = await self._execute_plan(execution_plan, context)
            
            # 4. 성능 메트릭 업데이트
            self._update_performance_stats(True, execution_plan)
            
            logger.info(f"SQL request processed successfully in {result.get('execution_time', 0):.2f}s")
            return result
            
        except Exception as e:
            logger.error(f"Failed to process SQL request: {str(e)}")
            self._update_performance_stats(False, None)
            raise
    
    async def _create_execution_plan(self, context: ExecutionContext) -> ExecutionPlan:
        """
        실행 계획 수립
        
        Args:
            context: 실행 컨텍스트
            
        Returns:
            ExecutionPlan: 수립된 실행 계획
        """
        complexity = context.complexity_score
        
        if complexity < 0.3:
            # 단순한 쿼리 - 최소한의 Agent만 사용
            return self._create_simple_plan(context)
        elif complexity < 0.7:
            # 중간 복잡도 - 표준 플로우
            return self._create_standard_plan(context)
        else:
            # 복잡한 쿼리 - 전체 Agent 동원
            return self._create_complex_plan(context)
    
    def _create_simple_plan(self, context: ExecutionContext) -> ExecutionPlan:
        """단순 쿼리용 실행 계획"""
        phases = [
            ExecutionPhase(
                name="validation",
                agent_tasks=[
                    AgentTask(
                        agent_name="schema_intelligence",
                        task_type="quick_analysis",
                        input_data={"query": context.query, "state": context.state}
                    )
                ]
            ),
            ExecutionPhase(
                name="generation",
                agent_tasks=[
                    AgentTask(
                        agent_name="query_architect", 
                        task_type="simple_generation",
                        input_data={"query": context.query, "context": context.user_context}
                    )
                ],
                dependencies=["validation"]
            )
        ]
        
        return ExecutionPlan(
            id=f"simple_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            phases=phases,
            mode=ExecutionMode.SEQUENTIAL,
            estimated_duration=5.0,
            success_criteria={"sql_generated": True, "validation_passed": True}
        )
    
    def _create_standard_plan(self, context: ExecutionContext) -> ExecutionPlan:
        """표준 복잡도 쿼리용 실행 계획"""
        phases = [
            ExecutionPhase(
                name="analysis", 
                agent_tasks=[
                    AgentTask(
                        agent_name="schema_intelligence",
                        task_type="full_analysis", 
                        input_data={"query": context.query, "state": context.state},
                        priority=TaskPriority.HIGH
                    )
                ]
            ),
            ExecutionPhase(
                name="exploration",
                agent_tasks=[
                    AgentTask(
                        agent_name="data_investigator",
                        task_type="explore_uncertainties",
                        input_data={"uncertainties": "from_analysis"},
                        priority=TaskPriority.MEDIUM
                    )
                ],
                dependencies=["analysis"]
            ),
            ExecutionPhase(
                name="generation",
                agent_tasks=[
                    AgentTask(
                        agent_name="query_architect",
                        task_type="optimized_generation", 
                        input_data={"analysis_result": "from_analysis", "exploration_result": "from_exploration"},
                        priority=TaskPriority.HIGH
                    )
                ],
                dependencies=["analysis", "exploration"]
            )
        ]
        
        return ExecutionPlan(
            id=f"standard_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            phases=phases,
            mode=ExecutionMode.HYBRID,
            estimated_duration=15.0,
            success_criteria={"sql_generated": True, "uncertainties_resolved": True}
        )
    
    def _create_complex_plan(self, context: ExecutionContext) -> ExecutionPlan:
        """복잡한 쿼리용 실행 계획"""
        phases = [
            ExecutionPhase(
                name="comprehensive_analysis",
                agent_tasks=[
                    AgentTask(
                        agent_name="schema_intelligence",
                        task_type="deep_analysis",
                        input_data={"query": context.query, "state": context.state},
                        priority=TaskPriority.CRITICAL
                    ),
                    AgentTask(
                        agent_name="data_investigator", 
                        task_type="preliminary_exploration",
                        input_data={"query": context.query},
                        priority=TaskPriority.HIGH
                    )
                ]
            ),
            ExecutionPhase(
                name="iterative_refinement",
                agent_tasks=[
                    AgentTask(
                        agent_name="query_architect",
                        task_type="draft_generation",
                        input_data={"analysis_result": "from_analysis"},
                        priority=TaskPriority.HIGH
                    ),
                    AgentTask(
                        agent_name="schema_intelligence",
                        task_type="validation_review", 
                        input_data={"draft_sql": "from_generation"},
                        priority=TaskPriority.MEDIUM
                    )
                ],
                dependencies=["comprehensive_analysis"]
            ),
            ExecutionPhase(
                name="optimization",
                agent_tasks=[
                    AgentTask(
                        agent_name="query_architect",
                        task_type="final_optimization",
                        input_data={"draft_sql": "from_refinement", "feedback": "from_validation"},
                        priority=TaskPriority.HIGH
                    )
                ],
                dependencies=["iterative_refinement"]
            ),
            ExecutionPhase(
                name="communication_check",
                agent_tasks=[
                    AgentTask(
                        agent_name="communication_specialist",
                        task_type="clarity_assessment",
                        input_data={"final_sql": "from_optimization", "original_query": context.query},
                        priority=TaskPriority.LOW
                    )
                ],
                dependencies=["optimization"]
            )
        ]
        
        return ExecutionPlan(
            id=f"complex_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            phases=phases,
            mode=ExecutionMode.ADAPTIVE,
            estimated_duration=30.0,
            success_criteria={"sql_generated": True, "optimization_completed": True, "validation_passed": True}
        )
    
    async def _execute_plan(self, plan: ExecutionPlan, context: ExecutionContext) -> Dict[str, Any]:
        """
        실행 계획 수행
        
        Args:
            plan: 실행 계획
            context: 실행 컨텍스트
            
        Returns:
            Dict: 실행 결과
        """
        start_time = datetime.now()
        results = {}
        
        logger.info(f"Executing plan '{plan.id}' with {len(plan.phases)} phases")
        
        try:
            for phase in plan.phases:
                # 의존성 검사
                if not self._check_dependencies(phase, results):
                    raise Exception(f"Dependencies not met for phase '{phase.name}'")
                
                # 단계 실행
                phase_result = await self._execute_phase(phase, context, results)
                results[phase.name] = phase_result
                
                logger.info(f"Phase '{phase.name}' completed successfully")
            
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            return {
                "success": True,
                "execution_time": execution_time,
                "plan_id": plan.id,
                "results": results,
                "performance": {
                    "estimated_duration": plan.estimated_duration,
                    "actual_duration": execution_time,
                    "efficiency": plan.estimated_duration / max(execution_time, 0.1)
                }
            }
            
        except Exception as e:
            logger.error(f"Plan execution failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "plan_id": plan.id,
                "partial_results": results
            }
    
    def _check_dependencies(self, phase: ExecutionPhase, completed_results: Dict) -> bool:
        """의존성 검사"""
        for dependency in phase.dependencies:
            if dependency not in completed_results:
                return False
        return True
    
    async def _execute_phase(self, phase: ExecutionPhase, context: ExecutionContext, 
                           completed_results: Dict) -> Dict[str, Any]:
        """단계 실행"""
        phase_results = {}
        
        # Agent 작업들을 병렬 또는 순차로 실행
        if len(phase.agent_tasks) == 1:
            # 단일 작업 - 직접 실행
            task = phase.agent_tasks[0]
            result = await self._execute_agent_task(task, context, completed_results)
            phase_results[task.task_type] = result
        else:
            # 복수 작업 - 병렬 실행
            tasks = []
            for agent_task in phase.agent_tasks:
                task_coroutine = self._execute_agent_task(agent_task, context, completed_results)
                tasks.append(task_coroutine)
            
            task_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, result in enumerate(task_results):
                task_name = phase.agent_tasks[i].task_type
                if isinstance(result, Exception):
                    logger.error(f"Task '{task_name}' failed: {str(result)}")
                    phase_results[task_name] = {"error": str(result)}
                else:
                    phase_results[task_name] = result
        
        return phase_results
    
    async def _execute_agent_task(self, task: AgentTask, context: ExecutionContext,
                                completed_results: Dict) -> Dict[str, Any]:
        """Agent 작업 실행"""
        agent = self.agents.get(task.agent_name)
        if not agent:
            raise Exception(f"Agent '{task.agent_name}' not found")
        
        # 입력 데이터 준비
        input_data = self._prepare_task_input(task, context, completed_results)
        
        # 메시지 생성
        message = AgentMessage(
            sender="master_orchestrator",
            receiver=task.agent_name,
            message_type=MessageType.REQUEST,
            content={
                "task_type": task.task_type,
                "input_data": input_data,
                "context": context.user_context
            },
            priority=task.priority.value,
            timeout=task.timeout
        )
        
        # Agent에게 작업 요청
        try:
            agent.status = AgentStatus.PROCESSING
            response = await agent.process_message(message)
            agent.status = AgentStatus.IDLE
            
            # 성능 통계 업데이트
            self._update_agent_utilization_stats(task.agent_name, True)
            
            return response.content
            
        except Exception as e:
            agent.status = AgentStatus.ERROR
            self._update_agent_utilization_stats(task.agent_name, False)
            logger.error(f"Agent task failed: {task.agent_name}.{task.task_type} - {str(e)}")
            raise
    
    def _prepare_task_input(self, task: AgentTask, context: ExecutionContext,
                          completed_results: Dict) -> Dict[str, Any]:
        """작업 입력 데이터 준비"""
        input_data = task.input_data.copy()
        
        # 이전 결과 참조 해결
        for key, value in input_data.items():
            if isinstance(value, str) and value.startswith("from_"):
                phase_name = value.replace("from_", "")
                if phase_name in completed_results:
                    input_data[key] = completed_results[phase_name]
        
        return input_data
    
    def _update_performance_stats(self, success: bool, plan: Optional[ExecutionPlan]):
        """성능 통계 업데이트"""
        self.performance_stats["total_executions"] += 1
        
        if success:
            self.performance_stats["successful_executions"] += 1
        
        if plan and success:
            # 평균 실행 시간 업데이트 로직 (실제 구현에서는 더 정교하게)
            pass
    
    def _update_agent_utilization_stats(self, agent_name: str, success: bool):
        """Agent 활용 통계 업데이트"""
        if agent_name in self.performance_stats["agent_utilization"]:
            stats = self.performance_stats["agent_utilization"][agent_name]
            stats["total_tasks"] += 1
            if success:
                stats["successful_tasks"] += 1
    
    def get_system_status(self) -> Dict[str, Any]:
        """시스템 상태 조회"""
        agent_statuses = {}
        for name, agent in self.agents.items():
            agent_statuses[name] = agent.get_status()
        
        return {
            "orchestrator": {
                "registered_agents": len(self.agents),
                "active_executions": len(self.active_executions),
                "performance_stats": self.performance_stats
            },
            "agents": agent_statuses,
            "timestamp": datetime.now().isoformat()
        }
    
    async def shutdown(self):
        """시스템 종료"""
        logger.info("Shutting down MasterOrchestrator...")
        
        # 모든 Agent 정리
        for agent in self.agents.values():
            await agent.cleanup()
        
        self.agents.clear()
        self.active_executions.clear()
        
        logger.info("MasterOrchestrator shutdown completed")