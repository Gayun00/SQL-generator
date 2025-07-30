"""
DynamicOrchestrator - 완전 동적 A2A 워크플로우 관리자

기존 MasterOrchestrator의 고정된 플로우 템플릿을 제거하고,
각 Agent의 실행 결과를 기반으로 다음에 실행할 Agent를 실시간으로 결정합니다.
중앙 집중식 구조를 유지하면서 최대한의 유연성과 효율성을 제공합니다.
"""

from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import logging
from datetime import datetime
import json

from .base_agent import BaseAgent, AgentMessage, MessageType, AgentStatus
from workflow.state import SQLGeneratorState

logger = logging.getLogger(__name__)

class ExecutionDecision(Enum):
    """실행 결정 유형"""
    CONTINUE = "continue"        # 다음 Agent로 계속 진행
    COMPLETE = "complete"        # 워크플로우 완료
    RETRY = "retry"             # 현재 Agent 재시도
    CLARIFY = "clarify"         # 사용자 설명 필요
    ERROR = "error"             # 오류로 인한 중단

@dataclass
class AgentExecutionResult:
    """Agent 실행 결과"""
    agent_name: str
    task_type: str
    success: bool
    result_data: Dict[str, Any]
    execution_time: float
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ExecutionContext:
    """동적 실행 컨텍스트"""
    query: str
    state: SQLGeneratorState
    executed_agents: Set[str] = field(default_factory=set)
    agent_results: Dict[str, AgentExecutionResult] = field(default_factory=dict)
    accumulated_insights: Dict[str, Any] = field(default_factory=dict)
    completion_criteria_met: Set[str] = field(default_factory=set)
    user_context: Dict[str, Any] = field(default_factory=dict)

@dataclass
class NextAgentSuggestion:
    """다음 Agent 제안"""
    agent_name: str
    task_type: str
    priority: int  # 1=highest, 5=lowest
    reason: str
    input_data: Dict[str, Any]
    required: bool = False  # 필수 실행 여부

class AgentResultAnalyzer:
    """Agent 결과 분석 및 다음 단계 결정"""
    
    @staticmethod
    def analyze_result_and_suggest_next_agents(
        agent_result: AgentExecutionResult, 
        context: ExecutionContext
    ) -> List[NextAgentSuggestion]:
        """
        Agent 실행 결과를 분석하여 다음에 실행할 Agent들을 제안
        
        Args:
            agent_result: 완료된 Agent의 실행 결과
            context: 현재 실행 컨텍스트
            
        Returns:
            List[NextAgentSuggestion]: 다음 Agent 제안 목록
        """
        suggestions = []
        agent_name = agent_result.agent_name
        result_data = agent_result.result_data
        
        # 🔍 SchemaIntelligence 결과 분석
        if agent_name == "schema_intelligence":
            suggestions.extend(
                AgentResultAnalyzer._analyze_schema_intelligence_result(result_data, context)
            )
        
        # 🕵️ DataInvestigator 결과 분석
        elif agent_name == "data_investigator":
            suggestions.extend(
                AgentResultAnalyzer._analyze_data_investigator_result(result_data, context)
            )
        
        # 🏗️ QueryArchitect 결과 분석
        elif agent_name == "query_architect":
            suggestions.extend(
                AgentResultAnalyzer._analyze_query_architect_result(result_data, context)
            )
        
        # 💬 CommunicationSpecialist 결과 분석
        elif agent_name == "communication_specialist":
            suggestions.extend(
                AgentResultAnalyzer._analyze_communication_specialist_result(result_data, context)
            )
        
        return suggestions
    
    @staticmethod
    def _analyze_schema_intelligence_result(
        result_data: Dict[str, Any], 
        context: ExecutionContext
    ) -> List[NextAgentSuggestion]:
        """SchemaIntelligence 결과 분석"""
        suggestions = []
        
        # 분석 결과 추출
        analysis_result = (
            result_data.get("full_analysis") or 
            result_data.get("quick_analysis") or 
            result_data.get("deep_analysis") or
            {}
        )
        
        if analysis_result.get("error"):
            # 분석 실패 시 커뮤니케이션 전문가에게 도움 요청
            suggestions.append(NextAgentSuggestion(
                agent_name="communication_specialist",
                task_type="generate_error_explanation",
                priority=2,
                reason=f"Schema analysis failed: {analysis_result['error']}",
                input_data={"error": analysis_result["error"], "query": context.query},
                required=True
            ))
            return suggestions
        
        uncertainty_analysis = analysis_result.get("uncertainty_analysis", {})
        has_uncertainty = uncertainty_analysis.get("has_uncertainty", False)
        confidence = uncertainty_analysis.get("confidence", 0.0)
        
        # 누적 인사이트 업데이트
        context.accumulated_insights.update({
            "schema_analysis": analysis_result,
            "uncertainty_level": uncertainty_analysis.get("uncertainty_level", "low"),
            "confidence_score": confidence
        })
        
        # 불확실성이 있고 신뢰도가 낮으면 탐색 필요
        if has_uncertainty and confidence < 0.7:
            uncertainties = uncertainty_analysis.get("uncertainties", [])
            suggestions.append(NextAgentSuggestion(
                agent_name="data_investigator",
                task_type="explore_uncertainties",
                priority=1,
                reason=f"Uncertainties detected (confidence: {confidence:.2f})",
                input_data={
                    "uncertainties": uncertainties,
                    "schema_analysis": analysis_result,
                    "query": context.query
                },
                required=True
            ))
        
        # 불확실성이 없거나 신뢰도가 높으면 바로 SQL 생성
        elif not has_uncertainty or confidence >= 0.8:
            suggestions.append(NextAgentSuggestion(
                agent_name="query_architect",
                task_type="generate_sql",
                priority=1,
                reason=f"High confidence analysis (confidence: {confidence:.2f})",
                input_data={
                    "schema_analysis": analysis_result,
                    "query": context.query,
                    "context": context.accumulated_insights
                },
                required=True
            ))
        
        # 중간 신뢰도인 경우 두 옵션 모두 제안 (우선순위로 구분)
        else:
            suggestions.extend([
                NextAgentSuggestion(
                    agent_name="data_investigator",
                    task_type="quick_exploration",
                    priority=2,
                    reason=f"Medium confidence, quick exploration recommended",
                    input_data={
                        "uncertainties": uncertainty_analysis.get("uncertainties", []),
                        "schema_analysis": analysis_result
                    }
                ),
                NextAgentSuggestion(
                    agent_name="query_architect",
                    task_type="generate_sql",
                    priority=3,
                    reason="Alternative: proceed with current analysis",
                    input_data={
                        "schema_analysis": analysis_result,
                        "query": context.query
                    }
                )
            ])
        
        return suggestions
    
    @staticmethod
    def _analyze_data_investigator_result(
        result_data: Dict[str, Any], 
        context: ExecutionContext
    ) -> List[NextAgentSuggestion]:
        """DataInvestigator 결과 분석"""
        suggestions = []
        
        exploration_result = (
            result_data.get("explore_uncertainties") or
            result_data.get("quick_exploration") or
            {}
        )
        
        if exploration_result.get("error"):
            # 탐색 실패 시 사용자 설명 요청
            suggestions.append(NextAgentSuggestion(
                agent_name="communication_specialist",
                task_type="generate_clarification",
                priority=1,
                reason=f"Data exploration failed: {exploration_result['error']}",
                input_data={
                    "exploration_error": exploration_result["error"],
                    "original_query": context.query,
                    "attempted_explorations": exploration_result.get("attempted_queries", [])
                },
                required=True
            ))
            return suggestions
        
        successful_explorations = exploration_result.get("successful_queries", 0)
        insights = exploration_result.get("insights", [])
        resolved_uncertainties = exploration_result.get("resolved_uncertainties", [])
        
        # 탐색 결과를 누적 인사이트에 추가
        context.accumulated_insights.update({
            "exploration_results": exploration_result,
            "resolved_uncertainties": resolved_uncertainties,
            "data_insights": insights
        })
        
        # 성공적인 탐색 결과가 있으면 SQL 생성으로 진행
        if successful_explorations > 0 and len(insights) > 0:
            suggestions.append(NextAgentSuggestion(
                agent_name="query_architect",
                task_type="generate_sql",
                priority=1,
                reason=f"Exploration successful ({successful_explorations} queries, {len(insights)} insights)",
                input_data={
                    "schema_analysis": context.accumulated_insights.get("schema_analysis"),
                    "exploration_results": exploration_result,
                    "query": context.query,
                    "context": context.accumulated_insights
                },
                required=True
            ))
        
        # 탐색이 부분적으로만 성공했으면 추가 탐색 또는 설명 요청
        elif successful_explorations > 0 and len(insights) == 0:
            suggestions.extend([
                NextAgentSuggestion(
                    agent_name="data_investigator",
                    task_type="deep_exploration",
                    priority=2,
                    reason="Partial success, deeper exploration needed",
                    input_data={
                        "previous_results": exploration_result,
                        "query": context.query
                    }
                ),
                NextAgentSuggestion(
                    agent_name="communication_specialist",
                    task_type="generate_clarification",
                    priority=3,
                    reason="Alternative: ask user for clarification",
                    input_data={
                        "partial_exploration": exploration_result,
                        "query": context.query
                    }
                )
            ])
        
        # 탐색이 완전히 실패했으면 사용자 설명 필요
        else:
            suggestions.append(NextAgentSuggestion(
                agent_name="communication_specialist",
                task_type="generate_clarification",
                priority=1,
                reason="Exploration failed, user clarification needed",
                input_data={
                    "failed_exploration": exploration_result,
                    "query": context.query,
                    "unresolved_uncertainties": exploration_result.get("unresolved_uncertainties", [])
                },
                required=True
            ))
        
        return suggestions
    
    @staticmethod
    def _analyze_query_architect_result(
        result_data: Dict[str, Any], 
        context: ExecutionContext
    ) -> List[NextAgentSuggestion]:
        """QueryArchitect 결과 분석"""
        suggestions = []
        
        # QueryArchitect 결과는 직접 result_data에 포함됨
        generation_result = result_data
        
        # 디버깅: QueryArchitect 결과 로깅
        logger.info(f"QueryArchitect result keys: {list(result_data.keys())}")
        logger.info(f"SQL query: {result_data.get('sql_query', 'None')[:100] if result_data.get('sql_query') else 'None'}")
        if 'execution_result' in result_data:
            logger.info(f"Execution result: {result_data.get('execution_result')}")
        if 'bigquery_result' in result_data:
            logger.info(f"BigQuery result: {result_data.get('bigquery_result')}")
        
        if generation_result.get("error"):
            # SQL 생성 실패
            error_msg = generation_result["error"]
            if "improvement" in result_data:
                # 이미 개선을 시도했는데도 실패했으면 사용자 도움 필요
                suggestions.append(NextAgentSuggestion(
                    agent_name="communication_specialist",
                    task_type="generate_error_explanation",
                    priority=1,
                    reason=f"SQL generation failed after improvement attempts: {error_msg}",
                    input_data={
                        "error": error_msg,
                        "query": context.query,
                        "attempted_sql": generation_result.get("attempted_sql"),
                        "improvement_attempts": result_data.get("improvement_history", [])
                    },
                    required=True
                ))
            else:
                # 첫 번째 실패면 개선 시도
                suggestions.append(NextAgentSuggestion(
                    agent_name="query_architect",
                    task_type="execute_with_improvements",
                    priority=1,
                    reason=f"Initial generation failed, trying improvements: {error_msg}",
                    input_data={
                        "failed_sql": generation_result.get("attempted_sql"),
                        "error_message": error_msg,
                        "original_query": context.query,
                        "context": context.accumulated_insights
                    },
                    required=True
                ))
            return suggestions
        
        sql_query = generation_result.get("sql_query", "")
        execution_result = generation_result.get("query_result", {})
        
        # SQL이 유효한지 확인 (SELECT로 시작하는지 등)
        if not sql_query or not sql_query.strip().upper().startswith(('SELECT', 'WITH')):
            logger.info(f"Invalid SQL generated: {sql_query[:200] if sql_query else 'None'}")
            # SQL이 제대로 생성되지 않음 - 커뮤니케이션으로 처리
            context.completion_criteria_met.add("sql_generation_failed")
            suggestions.append(NextAgentSuggestion(
                agent_name="communication_specialist",
                task_type="generate_error_explanation",
                priority=1,
                reason="SQL query generation failed or invalid",
                input_data={
                    "error": f"Generated content is not a valid SQL query: {sql_query[:100] if sql_query else 'None'}",
                    "query": context.query,
                    "context": context.accumulated_insights
                },
                required=True
            ))
            return suggestions
        
        # SQL이 성공적으로 실행됨
        if execution_result.get("success"):
            context.completion_criteria_met.add("sql_executed_successfully")
            context.accumulated_insights.update({
                "final_sql": sql_query,
                "execution_result": execution_result,
                "query_explanation": generation_result.get("explanation")
            })
            
            # 결과가 만족스러우면 완료, 아니면 커뮤니케이션 체크
            returned_rows = execution_result.get("returned_rows", 0)
            if returned_rows > 0:
                # 성공적인 결과 - 바로 완료 가능
                # 선택적으로 커뮤니케이션 체크 제안
                suggestions.append(NextAgentSuggestion(
                    agent_name="communication_specialist",
                    task_type="final_review",
                    priority=4,  # 낮은 우선순위 (선택사항)
                    reason=f"SQL executed successfully ({returned_rows} rows), optional final review",
                    input_data={
                        "sql_query": sql_query,
                        "execution_result": execution_result,
                        "original_query": context.query
                    }
                ))
            else:
                # 결과가 없음 - 확인 필요
                suggestions.append(NextAgentSuggestion(
                    agent_name="communication_specialist",
                    task_type="empty_result_explanation",
                    priority=2,
                    reason="SQL executed but returned no rows",
                    input_data={
                        "sql_query": sql_query,
                        "execution_result": execution_result,
                        "original_query": context.query
                    }
                ))
        
        # SQL 실행 실패
        elif sql_query and not execution_result.get("success"):
            error_message = execution_result.get("error", "Unknown execution error")
            
            # 개선 가능한 오류인지 확인
            if any(pattern in error_message.lower() for pattern in 
                   ["unrecognized name", "does not exist", "invalid", "syntax error"]):
                suggestions.append(NextAgentSuggestion(
                    agent_name="query_architect",
                    task_type="execute_with_improvements",
                    priority=1,
                    reason=f"SQL execution failed with fixable error: {error_message[:50]}...",
                    input_data={
                        "sql_query": sql_query,
                        "error_message": error_message,
                        "original_query": context.query,
                        "context": context.accumulated_insights
                    },
                    required=True
                ))
            else:
                # 개선하기 어려운 오류 - 사용자 설명
                suggestions.append(NextAgentSuggestion(
                    agent_name="communication_specialist",
                    task_type="generate_error_explanation",
                    priority=1,
                    reason=f"SQL execution failed with complex error: {error_message}",
                    input_data={
                        "sql_query": sql_query,
                        "error": error_message,
                        "original_query": context.query
                    },
                    required=True
                ))
        
        return suggestions
    
    @staticmethod
    def _analyze_communication_specialist_result(
        result_data: Dict[str, Any], 
        context: ExecutionContext
    ) -> List[NextAgentSuggestion]:
        """CommunicationSpecialist 결과 분석"""
        suggestions = []
        
        # 커뮤니케이션 결과는 보통 최종 단계이므로 완료 조건 확인
        if any(task in result_data for task in 
               ["final_review", "generate_error_explanation", "empty_result_explanation"]):
            context.completion_criteria_met.add("communication_completed")
            # 일반적으로 더 이상 진행할 필요 없음 (완료)
        
        elif "generate_clarification" in result_data:
            clarification_result = result_data["generate_clarification"]
            if clarification_result.get("clarification_questions"):
                context.completion_criteria_met.add("clarification_needed")
                # 실제 시스템에서는 여기서 사용자 입력을 기다림
        
        return suggestions  # 보통 빈 리스트 (더 이상 진행하지 않음)
    
    @staticmethod
    def should_terminate_workflow(context: ExecutionContext) -> Dict[str, Any]:
        """
        현재 컨텍스트를 기반으로 워크플로우 종료 여부 결정
        
        Args:
            context: 현재 실행 컨텍스트
            
        Returns:
            Dict: 워크플로우 종료 결정 정보
        """
        completion_status = {
            "should_terminate": False,
            "termination_reason": None,
            "reason": "",
            "final_result": {}
        }
        
        # 성공적인 SQL 실행 완료
        if "sql_executed_successfully" in context.completion_criteria_met:
            final_sql = context.accumulated_insights.get("final_sql")
            execution_result = context.accumulated_insights.get("execution_result")
            
            if final_sql and execution_result:
                completion_status.update({
                    "should_terminate": True,
                    "termination_reason": "success",
                    "reason": "SQL successfully generated and executed",
                    "final_result": {
                        "sql_query": final_sql,
                        "execution_result": execution_result,
                        "explanation": context.accumulated_insights.get("query_explanation"),
                        "insights": context.accumulated_insights.get("data_insights", [])
                    }
                })
        
        # 사용자 설명이 필요한 상황
        elif "clarification_needed" in context.completion_criteria_met:
            completion_status.update({
                "should_terminate": True,
                "termination_reason": "clarification_needed",
                "reason": "User clarification required",
                "final_result": {
                    "clarification_questions": context.accumulated_insights.get("clarification_questions"),
                    "reason": "Unable to proceed without additional information"
                }
            })
        
        # 커뮤니케이션 완료 (오류 설명 등)
        elif "communication_completed" in context.completion_criteria_met:
            completion_status.update({
                "should_terminate": True,
                "termination_reason": "explained",
                "reason": "Issue explained to user",
                "final_result": context.accumulated_insights
            })
        
        # SQL 생성 실패로 완료
        elif "sql_generation_failed" in context.completion_criteria_met:
            completion_status.update({
                "should_terminate": True,
                "termination_reason": "sql_generation_failed",
                "reason": "SQL generation failed, issue will be explained",
                "final_result": context.accumulated_insights
            })
        
        # 너무 많은 Agent가 실행되었으면 강제 완료
        elif len(context.executed_agents) >= 10:
            completion_status.update({
                "should_terminate": True,
                "termination_reason": "max_iterations_reached",
                "reason": "Maximum number of agent executions reached",
                "final_result": {
                    "partial_results": context.accumulated_insights,
                    "executed_agents": list(context.executed_agents)
                }
            })
        
        # 기본 완료 조건: 최소 2개 Agent가 실행되고 더 이상 제안이 없으면 완료
        elif len(context.executed_agents) >= 2:
            completion_status.update({
                "should_terminate": True,
                "termination_reason": "workflow_completed",
                "reason": f"Workflow completed with {len(context.executed_agents)} agents",
                "final_result": context.accumulated_insights
            })
        
        return completion_status


class DynamicOrchestrator:
    """완전 동적 A2A 워크플로우 관리자"""
    
    def __init__(self):
        self.agents: Dict[str, BaseAgent] = {}
        self.execution_history: List[Dict[str, Any]] = []
        self.performance_stats = {
            "total_executions": 0,
            "successful_executions": 0,
            "average_execution_time": 0.0,
            "agent_utilization": {}
        }
        
        logger.info("DynamicOrchestrator initialized")
    
    def register_agent(self, agent: BaseAgent):
        """Agent 등록"""
        self.agents[agent.name] = agent
        self.performance_stats["agent_utilization"][agent.name] = {
            "total_executions": 0,
            "successful_executions": 0,
            "average_response_time": 0.0
        }
        logger.info(f"Agent '{agent.name}' registered with dynamic orchestrator")
    
    def unregister_agent(self, agent_name: str):
        """Agent 등록 해제"""
        if agent_name in self.agents:
            del self.agents[agent_name]
            logger.info(f"Agent '{agent_name}' unregistered from dynamic orchestrator")
    
    async def execute_dynamic_workflow(self, query: str, user_context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        완전 동적 워크플로우 실행
        
        Args:
            query: 사용자 쿼리
            user_context: 사용자 컨텍스트 (선택사항)
            
        Returns:
            Dict: 실행 결과
        """
        start_time = datetime.now()
        
        # 실행 컨텍스트 초기화
        context = ExecutionContext(
            query=query,
            state={
                "userInput": query,
                "isValid": True,
                "reason": None,
                "schemaInfo": None,
                "sqlQuery": None,
                "explanation": None,
                "finalOutput": None
            },
            user_context=user_context or {}
        )
        
        logger.info(f"Starting dynamic workflow execution for query: '{query[:50]}...'")
        
        try:
            # 첫 번째 Agent 결정 (보통 SchemaIntelligence)
            current_suggestions = [NextAgentSuggestion(
                agent_name="schema_intelligence",
                task_type="full_analysis",
                priority=1,
                reason="Initial schema analysis",
                input_data={"query": query, "state": context.state},
                required=True
            )]
            
            max_iterations = 15  # 무한루프 방지
            iteration = 0
            
            # 동적 실행 루프
            while current_suggestions and iteration < max_iterations:
                iteration += 1
                logger.info(f"Dynamic execution iteration {iteration}")
                
                # 워크플로우 종료 여부 확인
                completion_status = AgentResultAnalyzer.should_terminate_workflow(context)
                if completion_status["should_terminate"]:
                    logger.info(f"Workflow should terminate: {completion_status['reason']}")
                    break
                
                # 가장 우선순위가 높은 필수 Agent 선택
                next_suggestion = self._select_next_agent(current_suggestions, context)
                if not next_suggestion:
                    logger.info("No more agents to execute, completing workflow")
                    break
                
                # Agent 실행
                logger.info(f"Executing {next_suggestion.agent_name}.{next_suggestion.task_type}")
                agent_result = await self._execute_agent(next_suggestion, context)
                
                # 결과 저장
                context.agent_results[next_suggestion.agent_name] = agent_result
                context.executed_agents.add(next_suggestion.agent_name)
                
                # 실행 성공 시 다음 Agent 제안 받기
                if agent_result.success:
                    current_suggestions = AgentResultAnalyzer.analyze_result_and_suggest_next_agents(
                        agent_result, context
                    )
                    logger.info(f"Next suggestions: {len(current_suggestions)} agents proposed")
                else:
                    # 실행 실패 시 오류 처리
                    logger.error(f"Agent execution failed: {agent_result.error}")
                    current_suggestions = self._handle_agent_failure(agent_result, context)
            
            # 최종 워크플로우 종료 상태 확인
            final_completion_status = AgentResultAnalyzer.should_terminate_workflow(context)
            
            # 디버깅: 종료 상태 로깅
            logger.info(f"Final termination reason: {final_completion_status['termination_reason']}")
            logger.info(f"Completion criteria met: {list(context.completion_criteria_met)}")
            logger.info(f"Accumulated insights keys: {list(context.accumulated_insights.keys())}")
            
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            # 실행 결과 구성
            result = {
                "success": final_completion_status["should_terminate"],
                "termination_reason": final_completion_status["termination_reason"],
                "execution_time": execution_time,
                "iterations": iteration,
                "executed_agents": list(context.executed_agents),
                "agent_results": {name: result.result_data for name, result in context.agent_results.items()},
                "final_result": final_completion_status["final_result"],
                "accumulated_insights": context.accumulated_insights,
                "execution_history": [
                    {
                        "agent_name": name,
                        "success": result.success,
                        "execution_time": result.execution_time,
                        "task_type": result.task_type
                    }
                    for name, result in context.agent_results.items()
                ]
            }
            
            # 성공적인 완료를 위한 편의 필드 추가
            if final_completion_status["termination_reason"] == "success":
                final_data = final_completion_status["final_result"]
                result.update({
                    "sqlQuery": final_data.get("sql_query"),
                    "explanation": final_data.get("explanation"),
                    "queryResults": final_data.get("execution_result"),
                    "finalOutput": self._format_final_output(final_data),
                    "isValid": True
                })
            
            # 성능 통계 업데이트
            self._update_performance_stats(result["success"], execution_time)
            
            logger.info(f"Dynamic workflow completed in {execution_time:.2f}s with {iteration} iterations")
            return result
            
        except Exception as e:
            logger.error(f"Dynamic workflow execution failed: {str(e)}")
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            return {
                "success": False,
                "error": str(e),
                "execution_time": execution_time,
                "executed_agents": list(context.executed_agents),
                "partial_results": context.accumulated_insights
            }
    
    def _select_next_agent(self, suggestions: List[NextAgentSuggestion], context: ExecutionContext) -> Optional[NextAgentSuggestion]:
        """다음 실행할 Agent 선택"""
        if not suggestions:
            return None
        
        # 필수 Agent 중 우선순위가 가장 높은 것 선택
        required_suggestions = [s for s in suggestions if s.required]
        if required_suggestions:
            return min(required_suggestions, key=lambda x: x.priority)
        
        # 필수가 아닌 경우 우선순위가 가장 높고 아직 실행하지 않은 것 선택
        available_suggestions = [
            s for s in suggestions 
            if s.agent_name in self.agents and s.agent_name not in context.executed_agents
        ]
        
        if available_suggestions:
            return min(available_suggestions, key=lambda x: x.priority)
        
        return None
    
    async def _execute_agent(self, suggestion: NextAgentSuggestion, context: ExecutionContext) -> AgentExecutionResult:
        """Agent 실행"""
        agent = self.agents.get(suggestion.agent_name)
        if not agent:
            return AgentExecutionResult(
                agent_name=suggestion.agent_name,
                task_type=suggestion.task_type,
                success=False,
                result_data={},
                execution_time=0,
                error=f"Agent '{suggestion.agent_name}' not found"
            )
        
        start_time = datetime.now()
        
        try:
            # 메시지 생성
            message = AgentMessage(
                sender="dynamic_orchestrator",
                receiver=suggestion.agent_name,
                message_type=MessageType.REQUEST,
                content={
                    "task_type": suggestion.task_type,
                    "input_data": suggestion.input_data,
                    "context": context.user_context,
                    "accumulated_insights": context.accumulated_insights
                },
                priority=suggestion.priority,
                timeout=60
            )
            
            # Agent 실행
            agent.status = AgentStatus.PROCESSING
            response = await agent.process_message(message)
            agent.status = AgentStatus.IDLE
            
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            # 통계 업데이트
            self._update_agent_stats(suggestion.agent_name, True, execution_time)
            
            return AgentExecutionResult(
                agent_name=suggestion.agent_name,
                task_type=suggestion.task_type,
                success=True,
                result_data=response.content,
                execution_time=execution_time
            )
            
        except Exception as e:
            agent.status = AgentStatus.ERROR
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            self._update_agent_stats(suggestion.agent_name, False, execution_time)
            
            return AgentExecutionResult(
                agent_name=suggestion.agent_name,
                task_type=suggestion.task_type,
                success=False,
                result_data={},
                execution_time=execution_time,
                error=str(e)
            )
    
    def _handle_agent_failure(self, failed_result: AgentExecutionResult, context: ExecutionContext) -> List[NextAgentSuggestion]:
        """Agent 실행 실패 처리"""
        # 커뮤니케이션 전문가에게 오류 설명 요청
        return [NextAgentSuggestion(
            agent_name="communication_specialist",
            task_type="generate_error_explanation",
            priority=1,
            reason=f"Agent {failed_result.agent_name} failed: {failed_result.error}",
            input_data={
                "error": failed_result.error,
                "failed_agent": failed_result.agent_name,
                "query": context.query
            },
            required=True
        )]
    
    def _format_final_output(self, final_data: Dict[str, Any]) -> str:
        """최종 출력 포맷팅"""
        sql_query = final_data.get("sql_query", "")
        execution_result = final_data.get("execution_result", {})
        explanation = final_data.get("explanation", "")
        
        output_parts = []
        
        if explanation:
            output_parts.append(f"📖 설명: {explanation}")
        
        if sql_query:
            output_parts.append(f"📋 생성된 SQL:\n```sql\n{sql_query}\n```")
        
        if execution_result.get("success"):
            rows = execution_result.get("returned_rows", 0)
            output_parts.append(f"✅ 실행 결과: {rows}개 행 반환")
            
            if execution_result.get("sample_data"):
                output_parts.append("📊 샘플 데이터:")
                # 실제 데이터 포맷팅은 구현에 따라 조정
        
        return "\n\n".join(output_parts)
    
    def _update_performance_stats(self, success: bool, execution_time: float):
        """성능 통계 업데이트"""
        self.performance_stats["total_executions"] += 1
        if success:
            self.performance_stats["successful_executions"] += 1
        
        # 평균 실행 시간 업데이트
        total = self.performance_stats["total_executions"]
        current_avg = self.performance_stats["average_execution_time"]
        self.performance_stats["average_execution_time"] = (
            (current_avg * (total - 1) + execution_time) / total
        )
    
    def _update_agent_stats(self, agent_name: str, success: bool, execution_time: float):
        """Agent별 통계 업데이트"""
        if agent_name in self.performance_stats["agent_utilization"]:
            stats = self.performance_stats["agent_utilization"][agent_name]
            stats["total_executions"] += 1
            if success:
                stats["successful_executions"] += 1
            
            # 평균 응답 시간 업데이트
            total = stats["total_executions"]
            current_avg = stats["average_response_time"]
            stats["average_response_time"] = (
                (current_avg * (total - 1) + execution_time) / total
            )
    
    def get_available_agents(self) -> List[str]:
        """등록된 Agent 목록 반환"""
        return list(self.agents.keys())
    
    def get_system_status(self) -> Dict[str, Any]:
        """시스템 상태 조회"""
        agent_statuses = {}
        for name, agent in self.agents.items():
            agent_statuses[name] = agent.get_status()
        
        return {
            "orchestrator_type": "dynamic",
            "registered_agents": len(self.agents),
            "performance_stats": self.performance_stats,
            "agents": agent_statuses,
            "timestamp": datetime.now().isoformat()
        }
    
    async def shutdown(self):
        """시스템 종료"""
        logger.info("Shutting down DynamicOrchestrator...")
        
        for agent in self.agents.values():
            await agent.cleanup()
        
        self.agents.clear()
        logger.info("DynamicOrchestrator shutdown completed")