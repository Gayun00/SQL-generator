#!/usr/bin/env python3
"""
A2A (Agent-to-Agent) 워크플로우 - MasterOrchestrator 기반

기존 Langgraph 워크플로우를 대체하여 순수 A2A 아키텍처로 동작합니다.
각 Agent의 결과에 따라 동적으로 플로우가 결정됩니다.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.master_orchestrator import MasterOrchestrator, ExecutionContext
from agents.schema_intelligence_agent import create_schema_intelligence_agent
from agents.query_architect_agent import create_query_architect_agent
from agents.data_investigator_agent import create_data_investigator_agent
from agents.communication_specialist_agent import create_communication_specialist_agent
from db.bigquery_client import bq_client
from rag.schema_embedder import schema_embedder
from rag.schema_retriever import schema_retriever


class A2AWorkflow:
    """A2A 워크플로우 관리 클래스"""
    
    def __init__(self):
        self.orchestrator = None
        self.initialized = False
    
    async def initialize(self):
        """시스템 초기화"""
        print("🔧 A2A 워크플로우 초기화 중...")
        
        try:
            # 스키마 정보 초기화
            schema_info = schema_embedder.initialize_with_cache(bq_client)
            if not schema_info:
                print("❌ 스키마 정보 초기화 실패")
                return False
            
            bq_client.schema_info = schema_info
            print(f"✅ 스키마 정보: {len(schema_info)}개 테이블")
            
            # 스키마 검색기 초기화
            if not schema_retriever.initialize():
                print("❌ 스키마 검색기 초기화 실패")
                return False
            
            print("✅ 스키마 검색기 초기화 완료")
            
            # MasterOrchestrator 및 Agent 초기화
            self.orchestrator = MasterOrchestrator()
            
            # 모든 Agent 등록
            agents = [
                create_schema_intelligence_agent(),
                create_query_architect_agent(),
                create_data_investigator_agent(),
                create_communication_specialist_agent()
            ]
            
            for agent in agents:
                self.orchestrator.register_agent(agent)
                print(f"✅ {agent.name} Agent 등록 완료")
            
            self.initialized = True
            print("🎉 A2A 워크플로우 초기화 완료!")
            return True
            
        except Exception as e:
            print(f"❌ 초기화 실패: {str(e)}")
            return False
    
    async def process_query(self, user_query: str) -> dict:
        """
        사용자 쿼리 처리 - 동적 A2A 플로우 실행
        
        Args:
            user_query: 사용자 입력 쿼리
            
        Returns:
            dict: 처리 결과
        """
        if not self.initialized:
            raise Exception("A2A 워크플로우가 초기화되지 않았습니다.")
        
        print(f"🚀 A2A 처리 시작: '{user_query}'")
        print("-" * 60)
        
        # ExecutionContext 생성
        initial_state = {
            "userInput": user_query,
            "isValid": True,
            "reason": None,
            "schemaInfo": None,
            "sqlQuery": None,
            "explanation": None,
            "finalOutput": None,
            "queryResults": None,
            "executionStatus": None,
            "uncertaintyAnalysis": None,
            "hasUncertainty": None,
            "explorationResults": None,
            "needsClarification": None,
            "clarificationQuestions": None,
            "clarificationSummary": None,
            "userAnswers": None
        }
        
        context = ExecutionContext(
            query=user_query,
            state=initial_state
        )
        
        try:
            # MasterOrchestrator를 통한 동적 A2A 실행
            result = await self.orchestrator.process_sql_request(context)
            
            print(f"✅ A2A 처리 완료! ({result.get('execution_time', 0):.2f}초)")
            print(f"🎛️ 전략: {result.get('execution_plan', {}).get('strategy', 'unknown')}")
            print(f"📊 단계: {len(result.get('execution_plan', {}).get('completed_phases', []))}개 완료")
            
            if result.get('execution_plan', {}).get('early_completion'):
                print("⚡ 조기 완료 - 불필요한 단계 스킵됨")
            
            return result
            
        except Exception as e:
            print(f"❌ A2A 처리 실패: {str(e)}")
            raise
    
    def get_system_status(self) -> dict:
        """시스템 상태 조회"""
        if not self.initialized:
            return {"status": "not_initialized"}
        
        return self.orchestrator.get_system_status()
    
    async def shutdown(self):
        """시스템 종료"""
        if self.orchestrator:
            await self.orchestrator.shutdown()
        print("👋 A2A 워크플로우 종료 완료")


# 편의 함수들
async def create_a2a_workflow():
    """A2A 워크플로우 생성 및 초기화"""
    workflow = A2AWorkflow()
    success = await workflow.initialize()
    
    if not success:
        raise Exception("A2A 워크플로우 초기화 실패")
    
    return workflow


async def process_single_query(user_query: str) -> dict:
    """단일 쿼리 처리 (편의 함수)"""
    workflow = await create_a2a_workflow()
    try:
        result = await workflow.process_query(user_query)
        return result
    finally:
        await workflow.shutdown()


if __name__ == "__main__":
    import asyncio
    
    async def interactive_mode():
        """대화형 모드"""
        workflow = await create_a2a_workflow()
        
        print("\n🚀 A2A SQL Generator 시작!")
        print("=" * 60)
        print("💡 특징: Agent 결과에 따라 동적으로 플로우가 조정됩니다")
        print("   • 불확실성 없음 → 탐색 단계 스킵")
        print("   • SQL 첫 실행 성공 → 조기 완료")
        print("   • 실행 실패 → 자동 개선 단계 추가")
        print("=" * 60)
        
        try:
            while True:
                user_input = input("\n💬 SQL 생성 요청: ").strip()
                
                if user_input.lower() in ['quit', 'exit', '종료']:
                    print("👋 A2A 워크플로우를 종료합니다.")
                    break
                
                if not user_input:
                    print("⚠️ 입력이 비어있습니다.")
                    continue
                
                try:
                    result = await workflow.process_query(user_input)
                    
                    # 결과 출력
                    print("\n" + "=" * 60)
                    print("🎯 A2A 실행 결과:")
                    
                    if result.get("success"):
                        execution_plan = result.get("execution_plan", {})
                        print(f"✅ 성공 ({execution_plan.get('strategy', 'unknown')} 전략)")
                        print(f"📊 완료된 단계: {', '.join(execution_plan.get('completed_phases', []))}")
                        
                        if execution_plan.get('early_completion'):
                            print("⚡ 조기 완료됨 - 효율적인 처리!")
                        
                        # Agent 결과 요약
                        results = result.get("results", {})
                        for phase_name, phase_result in results.items():
                            print(f"\n📋 {phase_name}:")
                            for task_name, task_result in phase_result.items():
                                if isinstance(task_result, dict):
                                    status = "✅" if not task_result.get("error") else "❌"
                                    print(f"   {task_name}: {status}")
                                    
                                    # 중요 정보 표시
                                    if task_name == "full_analysis" and task_result.get("uncertainty_analysis"):
                                        ua = task_result["uncertainty_analysis"]
                                        print(f"      불확실성: {ua.get('has_uncertainty', False)}")
                                        print(f"      신뢰도: {ua.get('confidence', 0):.2f}")
                                    
                                    elif "sql_query" in task_result:
                                        sql = task_result.get("sql_query", "")
                                        print(f"      SQL: {sql[:50]}{'...' if len(sql) > 50 else ''}")
                                        
                                        if task_result.get("query_result", {}).get("success"):
                                            rows = task_result["query_result"].get("returned_rows", 0)
                                            print(f"      실행 결과: {rows}개 행")
                    else:
                        print(f"❌ 실패: {result.get('error', 'Unknown error')}")
                    
                except Exception as e:
                    print(f"❌ 처리 중 오류: {str(e)}")
                
        finally:
            await workflow.shutdown()
    
    async def test_mode():
        """테스트 모드"""
        test_queries = [
            "users 테이블의 모든 데이터를 보여줘",
            "최근 일주일간 가장 많은 금액을 결제한 유저 상위 5명을 보여줘",
            "월별 카테고리별 매출 현황을 분석해줘"
        ]
        
        workflow = await create_a2a_workflow()
        
        try:
            for i, query in enumerate(test_queries, 1):
                print(f"\n🧪 테스트 {i}: {query}")
                print("=" * 60)
                
                result = await workflow.process_query(query)
                
                if result.get("success"):
                    strategy = result.get("execution_plan", {}).get("strategy", "unknown")
                    phases = result.get("execution_plan", {}).get("completed_phases", [])
                    early = result.get("execution_plan", {}).get("early_completion", False)
                    
                    print(f"✅ 성공 - {strategy} 전략, {len(phases)}단계")
                    if early:
                        print("⚡ 조기 완료!")
                else:
                    print(f"❌ 실패: {result.get('error')}")
                
        finally:
            await workflow.shutdown()
    
    # 실행 모드 선택
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        asyncio.run(test_mode())
    else:
        asyncio.run(interactive_mode())