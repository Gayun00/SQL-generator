"""
Hybrid Nodes - 기존 노드와 Agent 시스템을 연결하는 브리지

기존 workflow에서 새로운 Agent 시스템을 호출할 수 있도록
호환성을 제공하는 래퍼 노드들입니다.
"""

import logging
from typing import Optional
from workflow.state import SQLGeneratorState
from agents.hybrid_manager import HybridManager, ExecutionMode, create_hybrid_manager

logger = logging.getLogger(__name__)

# 전역 HybridManager 인스턴스
_hybrid_manager: Optional[HybridManager] = None

def get_hybrid_manager() -> HybridManager:
    """전역 HybridManager 인스턴스 반환"""
    global _hybrid_manager
    if _hybrid_manager is None:
        _hybrid_manager = create_hybrid_manager(ExecutionMode.PARALLEL_COMPARE)
        logger.info("Global HybridManager initialized")
    return _hybrid_manager

async def hybrid_sql_analyzer(state: SQLGeneratorState) -> SQLGeneratorState:
    """
    Hybrid SQL Analyzer - 기존 sql_analyzer와 SchemaIntelligence Agent 병행 실행
    
    기존 sql_analyzer 노드를 대체하여 Agent 시스템과 비교 실행할 수 있습니다.
    """
    logger.info("🔄 Hybrid SQL Analyzer 시작 - Legacy vs Agent 비교 실행")
    
    try:
        # HybridManager를 통해 실행
        manager = get_hybrid_manager()
        result_state, comparison = await manager.execute_schema_analysis(state)
        
        # 비교 결과가 있으면 로깅
        if comparison:
            logger.info(f"📊 비교 결과: {comparison.recommendation}")
            logger.info(f"   - 성능 승자: {comparison.performance_winner}")
            logger.info(f"   - 정확도 일치: {comparison.accuracy_match}")
            logger.info(f"   - 실행 시간: Legacy {comparison.legacy_time:.2f}s vs Agent {comparison.agent_time:.2f}s")
            
            # 상태에 비교 정보 추가 (디버깅용)
            result_state["hybridComparison"] = {
                "performance_winner": comparison.performance_winner,
                "accuracy_match": comparison.accuracy_match,
                "legacy_time": comparison.legacy_time,
                "agent_time": comparison.agent_time,
                "recommendation": comparison.recommendation
            }
        
        logger.info("✅ Hybrid SQL Analyzer 완료")
        return result_state
        
    except Exception as e:
        logger.error(f"❌ Hybrid SQL Analyzer 실패: {str(e)}")
        
        # 실패시 기존 노드로 대체
        logger.info("🔄 기존 sql_analyzer로 대체 실행")
        from workflow.nodes import sql_analyzer
        return await sql_analyzer(state)

async def agent_only_sql_analyzer(state: SQLGeneratorState) -> SQLGeneratorState:
    """Agent만 사용하는 SQL Analyzer"""
    logger.info("🤖 Agent-only SQL Analyzer 시작")
    
    try:
        # Agent 전용 모드로 실행
        manager = get_hybrid_manager()
        manager.config.execution_mode = ExecutionMode.AGENT_ONLY
        
        result_state, _ = await manager.execute_schema_analysis(state)
        logger.info("✅ Agent-only SQL Analyzer 완료")
        return result_state
        
    except Exception as e:
        logger.error(f"❌ Agent-only SQL Analyzer 실패: {str(e)}")
        
        # 실패시 기존 노드로 대체
        logger.info("🔄 기존 sql_analyzer로 대체 실행")
        from workflow.nodes import sql_analyzer
        return await sql_analyzer(state)

async def legacy_only_sql_analyzer(state: SQLGeneratorState) -> SQLGeneratorState:
    """Legacy 노드만 사용하는 SQL Analyzer"""
    logger.info("🏛️ Legacy-only SQL Analyzer 시작")
    
    try:
        # Legacy 전용 모드로 실행
        manager = get_hybrid_manager()
        manager.config.execution_mode = ExecutionMode.LEGACY_ONLY
        
        result_state, _ = await manager.execute_schema_analysis(state)
        logger.info("✅ Legacy-only SQL Analyzer 완료")
        return result_state
        
    except Exception as e:
        logger.error(f"❌ Legacy-only SQL Analyzer 실패: {str(e)}")
        raise

def get_hybrid_performance_report() -> dict:
    """Hybrid 시스템 성능 리포트 조회"""
    try:
        manager = get_hybrid_manager()
        return manager.get_performance_report()
    except Exception as e:
        logger.error(f"성능 리포트 조회 실패: {str(e)}")
        return {"error": str(e)}

def get_recent_hybrid_comparisons(limit: int = 5) -> list:
    """최근 Hybrid 비교 결과 조회"""
    try:
        manager = get_hybrid_manager()
        return manager.get_recent_comparisons(limit)
    except Exception as e:
        logger.error(f"비교 결과 조회 실패: {str(e)}")
        return []

async def cleanup_hybrid_system():
    """Hybrid 시스템 정리"""
    global _hybrid_manager
    if _hybrid_manager:
        await _hybrid_manager.cleanup()
        _hybrid_manager = None
        logger.info("Hybrid 시스템 정리 완료")

# 설정 함수들
def set_hybrid_mode(mode: ExecutionMode):
    """Hybrid 실행 모드 설정"""
    manager = get_hybrid_manager()
    manager.config.execution_mode = mode
    logger.info(f"Hybrid 모드 변경: {mode.value}")

def enable_hybrid_comparison(enable: bool = True):
    """Hybrid 비교 모드 활성화/비활성화"""
    manager = get_hybrid_manager()
    manager.config.enable_comparison = enable
    logger.info(f"Hybrid 비교 모드: {'활성화' if enable else '비활성화'}")

def enable_hybrid_fallback(enable: bool = True):
    """Hybrid Fallback 모드 활성화/비활성화"""
    manager = get_hybrid_manager()
    manager.config.enable_fallback = enable
    logger.info(f"Hybrid Fallback 모드: {'활성화' if enable else '비활성화'}")

# 편의 함수들
def switch_to_comparison_mode():
    """비교 모드로 전환"""
    set_hybrid_mode(ExecutionMode.PARALLEL_COMPARE)
    enable_hybrid_comparison(True)

def switch_to_agent_mode():
    """Agent 전용 모드로 전환"""
    set_hybrid_mode(ExecutionMode.AGENT_ONLY)

def switch_to_legacy_mode():
    """Legacy 전용 모드로 전환"""
    set_hybrid_mode(ExecutionMode.LEGACY_ONLY)

def switch_to_production_mode():
    """프로덕션 모드로 전환 (Fallback)"""
    set_hybrid_mode(ExecutionMode.FALLBACK)
    enable_hybrid_fallback(True)