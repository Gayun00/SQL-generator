"""
Hybrid Manager - 기존 노드와 새로운 Agent 시스템 병행 실행 관리

기존 workflow 노드들과 새로운 A2A Agent들을 안전하게 병행 실행하고,
성능을 비교하여 점진적 전환을 지원합니다.
"""

from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import logging
from datetime import datetime, timedelta
import json

from .base_agent import BaseAgent, AgentMessage, MessageType
from .master_orchestrator import MasterOrchestrator, ExecutionContext
from .communication_hub import CommunicationHub, create_default_hub
from .schema_intelligence_agent import create_schema_intelligence_agent
from workflow.state import SQLGeneratorState

logger = logging.getLogger(__name__)

class ExecutionMode(Enum):
    """실행 모드"""
    LEGACY_ONLY = "legacy_only"        # 기존 노드만
    AGENT_ONLY = "agent_only"          # Agent만
    PARALLEL_COMPARE = "parallel_compare"  # 병렬 실행 후 비교
    A_B_TEST = "a_b_test"              # A/B 테스트
    FALLBACK = "fallback"              # Agent 실패시 노드로 대체

@dataclass
class ComparisonResult:
    """비교 결과"""
    legacy_result: Dict[str, Any]
    agent_result: Dict[str, Any]
    legacy_time: float
    agent_time: float
    accuracy_match: bool
    performance_winner: str  # "legacy" or "agent"
    confidence_score: float
    recommendation: str

@dataclass 
class HybridConfig:
    """Hybrid 설정"""
    execution_mode: ExecutionMode = ExecutionMode.PARALLEL_COMPARE
    enable_comparison: bool = True
    enable_fallback: bool = True
    timeout_seconds: int = 30
    max_retries: int = 2
    performance_threshold: float = 2.0  # Agent가 이것보다 느리면 legacy 사용
    accuracy_threshold: float = 0.8     # 정확도 임계값

class HybridManager:
    """Hybrid 실행 관리자"""
    
    def __init__(self, config: Optional[HybridConfig] = None):
        self.config = config or HybridConfig()
        
        # A2A 시스템 초기화
        self.orchestrator = MasterOrchestrator()
        self.communication_hub = create_default_hub()
        
        # Agent 등록
        self.schema_agent = create_schema_intelligence_agent()
        self.orchestrator.register_agent(self.schema_agent)
        self.communication_hub.register_agent(self.schema_agent)
        
        # QueryArchitect Agent 등록
        from .query_architect_agent import create_query_architect_agent
        self.query_agent = create_query_architect_agent()
        self.orchestrator.register_agent(self.query_agent)
        self.communication_hub.register_agent(self.query_agent)
        
        # 통계 추적
        self.execution_stats = {
            "total_executions": 0,
            "legacy_wins": 0,
            "agent_wins": 0,
            "failures": 0,
            "average_legacy_time": 0.0,
            "average_agent_time": 0.0
        }
        
        self.comparison_history: List[ComparisonResult] = []
        
        logger.info(f"HybridManager initialized with mode: {self.config.execution_mode.value}")
    
    async def execute_schema_analysis(self, state: SQLGeneratorState) -> Tuple[SQLGeneratorState, Optional[ComparisonResult]]:
        """
        스키마 분석 실행 - Hybrid 모드로
        
        Args:
            state: 현재 상태
            
        Returns:
            Tuple[결과 상태, 비교 결과 (있는 경우)]
        """
        mode = self.config.execution_mode
        
        if mode == ExecutionMode.LEGACY_ONLY:
            return await self._execute_legacy_only(state), None
        
        elif mode == ExecutionMode.AGENT_ONLY:
            return await self._execute_agent_only(state), None
        
        elif mode == ExecutionMode.PARALLEL_COMPARE:
            return await self._execute_parallel_compare(state)
        
        elif mode == ExecutionMode.A_B_TEST:
            return await self._execute_a_b_test(state)
        
        elif mode == ExecutionMode.FALLBACK:
            return await self._execute_with_fallback(state)
        
        else:
            logger.warning(f"Unknown execution mode: {mode}")
            return await self._execute_legacy_only(state), None
    
    async def _execute_legacy_only(self, state: SQLGeneratorState) -> SQLGeneratorState:
        """기존 노드만 실행"""
        logger.info("Executing legacy sql_analyzer only")
        
        try:
            # 기존 sql_analyzer 노드 임포트 및 실행
            from workflow.nodes import sql_analyzer
            
            start_time = datetime.now()
            result = await sql_analyzer(state)
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # 통계 업데이트
            self._update_stats("legacy", execution_time, True)
            
            logger.info(f"Legacy execution completed in {execution_time:.2f}s")
            return result
            
        except Exception as e:
            logger.error(f"Legacy execution failed: {str(e)}")
            self._update_stats("legacy", 0, False)
            raise
    
    async def _execute_agent_only(self, state: SQLGeneratorState) -> SQLGeneratorState:
        """Agent만 실행"""
        logger.info("Executing SchemaIntelligence Agent only")
        
        try:
            start_time = datetime.now()
            
            # ExecutionContext 생성
            context = ExecutionContext(
                query=state.get("userInput", ""),
                state=state
            )
            
            # Agent 실행
            result = await self.orchestrator.process_sql_request(context)
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # 결과를 SQLGeneratorState 형식으로 변환
            converted_result = self._convert_agent_result_to_state(state, result)
            
            # 통계 업데이트
            self._update_stats("agent", execution_time, True)
            
            logger.info(f"Agent execution completed in {execution_time:.2f}s")
            return converted_result
            
        except Exception as e:
            logger.error(f"Agent execution failed: {str(e)}")
            self._update_stats("agent", 0, False)
            raise
    
    async def _execute_parallel_compare(self, state: SQLGeneratorState) -> Tuple[SQLGeneratorState, ComparisonResult]:
        """병렬 실행 후 비교"""
        logger.info("Executing parallel comparison between legacy and agent")
        
        try:
            # 병렬 실행
            legacy_task = asyncio.create_task(self._timed_legacy_execution(state))
            agent_task = asyncio.create_task(self._timed_agent_execution(state))
            
            # 결과 대기
            (legacy_result, legacy_time), (agent_result, agent_time) = await asyncio.gather(
                legacy_task, agent_task, return_exceptions=True
            )
            
            # 예외 처리
            if isinstance(legacy_result, Exception):
                logger.error(f"Legacy execution failed: {legacy_result}")
                return await self._execute_agent_only(state), None
            
            if isinstance(agent_result, Exception):
                logger.error(f"Agent execution failed: {agent_result}")
                return await self._execute_legacy_only(state), None
            
            # 결과 비교
            comparison = self._compare_results(legacy_result, agent_result, legacy_time, agent_time)
            
            # 더 나은 결과 선택
            if comparison.performance_winner == "agent" and comparison.accuracy_match:
                selected_result = agent_result
                logger.info("Agent result selected based on comparison")
            else:
                selected_result = legacy_result
                logger.info("Legacy result selected based on comparison")
            
            # 통계 업데이트
            self.execution_stats["total_executions"] += 1
            if comparison.performance_winner == "agent":
                self.execution_stats["agent_wins"] += 1
            else:
                self.execution_stats["legacy_wins"] += 1
            
            # 비교 히스토리에 추가
            self.comparison_history.append(comparison)
            
            return selected_result, comparison
            
        except Exception as e:
            logger.error(f"Parallel comparison failed: {str(e)}")
            return await self._execute_legacy_only(state), None
    
    async def _execute_a_b_test(self, state: SQLGeneratorState) -> Tuple[SQLGeneratorState, Optional[ComparisonResult]]:
        """A/B 테스트 실행"""
        # 간단한 A/B 테스트: 50/50 확률로 선택
        import random
        
        if random.random() < 0.5:
            logger.info("A/B Test: Selected legacy execution")
            result = await self._execute_legacy_only(state)
            return result, None
        else:
            logger.info("A/B Test: Selected agent execution")
            result = await self._execute_agent_only(state)
            return result, None
    
    async def _execute_with_fallback(self, state: SQLGeneratorState) -> Tuple[SQLGeneratorState, Optional[ComparisonResult]]:
        """Agent 실행, 실패시 Legacy로 대체"""
        logger.info("Executing agent with legacy fallback")
        
        try:
            # Agent 먼저 시도
            result = await self._execute_agent_only(state)
            logger.info("Agent execution successful, no fallback needed")
            return result, None
            
        except Exception as e:
            logger.warning(f"Agent execution failed, falling back to legacy: {str(e)}")
            
            try:
                result = await self._execute_legacy_only(state)
                logger.info("Fallback to legacy successful")
                return result, None
                
            except Exception as fallback_error:
                logger.error(f"Both agent and legacy failed: {str(fallback_error)}")
                raise
    
    async def _timed_legacy_execution(self, state: SQLGeneratorState) -> Tuple[SQLGeneratorState, float]:
        """시간 측정이 포함된 Legacy 실행"""
        start_time = datetime.now()
        result = await self._execute_legacy_only(state)
        execution_time = (datetime.now() - start_time).total_seconds()
        return result, execution_time
    
    async def _timed_agent_execution(self, state: SQLGeneratorState) -> Tuple[SQLGeneratorState, float]:
        """시간 측정이 포함된 Agent 실행"""
        start_time = datetime.now()
        result = await self._execute_agent_only(state)
        execution_time = (datetime.now() - start_time).total_seconds()
        return result, execution_time
    
    def _convert_agent_result_to_state(self, original_state: SQLGeneratorState, agent_result: Dict[str, Any]) -> SQLGeneratorState:
        """Agent 결과를 SQLGeneratorState 형식으로 변환"""
        try:
            # Agent 결과에서 분석 정보 추출
            results = agent_result.get("results", {})
            analysis_data = None
            
            # 분석 결과 찾기
            for phase_name, phase_result in results.items():
                if "analysis" in phase_name.lower():
                    for task_name, task_result in phase_result.items():
                        if isinstance(task_result, dict) and "has_uncertainty" in task_result:
                            analysis_data = task_result
                            break
                    if analysis_data:
                        break
            
            if not analysis_data:
                # 기본 분석 데이터 생성
                analysis_data = {
                    "has_uncertainty": False,
                    "uncertainties": [],
                    "confidence": 0.8,
                    "analysis_type": "agent_conversion"
                }
            
            return {
                **original_state,
                "uncertaintyAnalysis": analysis_data,
                "hasUncertainty": analysis_data.get("has_uncertainty", False)
            }
            
        except Exception as e:
            logger.error(f"Failed to convert agent result: {str(e)}")
            return {
                **original_state,
                "uncertaintyAnalysis": {
                    "has_uncertainty": True,
                    "uncertainties": [],
                    "confidence": 0.0,
                    "error": f"변환 실패: {str(e)}"
                },
                "hasUncertainty": True
            }
    
    def _compare_results(self, legacy_result: SQLGeneratorState, agent_result: SQLGeneratorState,
                        legacy_time: float, agent_time: float) -> ComparisonResult:
        """결과 비교"""
        
        # 기본 정확도 비교
        legacy_analysis = legacy_result.get("uncertaintyAnalysis", {})
        agent_analysis = agent_result.get("uncertaintyAnalysis", {})
        
        # 불확실성 탐지 결과 비교
        legacy_has_uncertainty = legacy_result.get("hasUncertainty", False)
        agent_has_uncertainty = agent_result.get("hasUncertainty", False)
        
        accuracy_match = legacy_has_uncertainty == agent_has_uncertainty
        
        # 성능 비교
        performance_winner = "agent" if agent_time < legacy_time else "legacy"
        time_difference = abs(agent_time - legacy_time)
        
        # 신뢰도 점수 계산
        legacy_confidence = legacy_analysis.get("confidence", 0.5)
        agent_confidence = agent_analysis.get("confidence", 0.5)
        
        confidence_score = agent_confidence if performance_winner == "agent" else legacy_confidence
        
        # 추천사항 생성
        recommendation = self._generate_recommendation(
            accuracy_match, performance_winner, time_difference, 
            legacy_confidence, agent_confidence
        )
        
        return ComparisonResult(
            legacy_result=legacy_analysis,
            agent_result=agent_analysis,
            legacy_time=legacy_time,
            agent_time=agent_time,
            accuracy_match=accuracy_match,
            performance_winner=performance_winner,
            confidence_score=confidence_score,
            recommendation=recommendation
        )
    
    def _generate_recommendation(self, accuracy_match: bool, performance_winner: str,
                               time_difference: float, legacy_confidence: float,
                               agent_confidence: float) -> str:
        """추천사항 생성"""
        recommendations = []
        
        if accuracy_match:
            recommendations.append("✅ 정확도 일치")
        else:
            recommendations.append("⚠️ 정확도 불일치 - 추가 검토 필요")
        
        if performance_winner == "agent":
            if time_difference > 1.0:
                recommendations.append(f"🚀 Agent가 {time_difference:.1f}초 빠름")
            else:
                recommendations.append("⚡ Agent 성능 우수")
        else:
            if time_difference > 1.0:
                recommendations.append(f"🐌 Agent가 {time_difference:.1f}초 느림")
            else:
                recommendations.append("📊 Legacy 성능 우수")
        
        if agent_confidence > legacy_confidence + 0.1:
            recommendations.append("🎯 Agent 신뢰도 높음")
        elif legacy_confidence > agent_confidence + 0.1:
            recommendations.append("🔒 Legacy 신뢰도 높음")
        
        return " | ".join(recommendations)
    
    def _update_stats(self, executor: str, execution_time: float, success: bool):
        """통계 업데이트"""
        if not success:
            self.execution_stats["failures"] += 1
            return
        
        if executor == "legacy":
            current_avg = self.execution_stats["average_legacy_time"]
            total_legacy = self.execution_stats["legacy_wins"] + 1
            self.execution_stats["average_legacy_time"] = (
                (current_avg * (total_legacy - 1) + execution_time) / total_legacy
            )
        else:
            current_avg = self.execution_stats["average_agent_time"]
            total_agent = self.execution_stats["agent_wins"] + 1
            self.execution_stats["average_agent_time"] = (
                (current_avg * (total_agent - 1) + execution_time) / total_agent
            )
    
    def get_performance_report(self) -> Dict[str, Any]:
        """성능 리포트 생성"""
        stats = self.execution_stats
        total = stats["total_executions"]
        
        if total == 0:
            return {"message": "실행 이력이 없습니다."}
        
        agent_win_rate = (stats["agent_wins"] / total) * 100 if total > 0 else 0
        legacy_win_rate = (stats["legacy_wins"] / total) * 100 if total > 0 else 0
        
        return {
            "총 실행 횟수": total,
            "Agent 승률": f"{agent_win_rate:.1f}%",
            "Legacy 승률": f"{legacy_win_rate:.1f}%",
            "실패율": f"{(stats['failures'] / (total + stats['failures'])) * 100:.1f}%",
            "평균 실행 시간": {
                "Legacy": f"{stats['average_legacy_time']:.2f}초",
                "Agent": f"{stats['average_agent_time']:.2f}초"
            },
            "성능 개선": {
                "시간 단축": f"{max(0, stats['average_legacy_time'] - stats['average_agent_time']):.2f}초",
                "개선율": f"{((stats['average_legacy_time'] - stats['average_agent_time']) / max(stats['average_legacy_time'], 0.1)) * 100:.1f}%"
            },
            "추천": self._get_system_recommendation()
        }
    
    def _get_system_recommendation(self) -> str:
        """시스템 추천사항"""
        stats = self.execution_stats
        
        if stats["total_executions"] < 5:
            return "더 많은 테스트 데이터 필요"
        
        agent_win_rate = stats["agent_wins"] / stats["total_executions"]
        avg_time_improvement = stats["average_legacy_time"] - stats["average_agent_time"]
        
        if agent_win_rate > 0.8 and avg_time_improvement > 0.5:
            return "🎉 Agent 시스템으로 전환 권장"
        elif agent_win_rate > 0.6:
            return "⚖️ Agent 시스템 추가 최적화 후 전환 고려"
        else:
            return "🔧 Agent 시스템 개선 필요"
    
    def get_recent_comparisons(self, limit: int = 5) -> List[Dict[str, Any]]:
        """최근 비교 결과 조회"""
        recent = self.comparison_history[-limit:]
        
        return [
            {
                "성능_승자": comp.performance_winner,
                "정확도_일치": comp.accuracy_match,
                "실행_시간": {
                    "Legacy": f"{comp.legacy_time:.2f}초",
                    "Agent": f"{comp.agent_time:.2f}초"
                },
                "신뢰도": f"{comp.confidence_score:.2f}",
                "추천사항": comp.recommendation
            }
            for comp in recent
        ]
    
    async def cleanup(self):
        """정리 작업"""
        await self.orchestrator.shutdown()
        await self.communication_hub.cleanup()
        logger.info("HybridManager cleanup completed")

# 편의 함수들
def create_hybrid_manager(mode: ExecutionMode = ExecutionMode.PARALLEL_COMPARE) -> HybridManager:
    """Hybrid Manager 생성"""
    config = HybridConfig(execution_mode=mode)
    return HybridManager(config)

def create_development_hybrid_manager() -> HybridManager:
    """개발용 Hybrid Manager (디버깅 최적화)"""
    config = HybridConfig(
        execution_mode=ExecutionMode.PARALLEL_COMPARE,
        enable_comparison=True,
        timeout_seconds=60,  # 개발시 넉넉한 타임아웃
        max_retries=1
    )
    return HybridManager(config)

def create_production_hybrid_manager() -> HybridManager:
    """프로덕션용 Hybrid Manager (안정성 최우선)"""
    config = HybridConfig(
        execution_mode=ExecutionMode.FALLBACK,  # 안전한 fallback 모드
        enable_fallback=True,
        timeout_seconds=15,  # 빠른 응답
        max_retries=3
    )
    return HybridManager(config)