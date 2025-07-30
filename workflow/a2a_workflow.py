#!/usr/bin/env python3
"""
A2A (Agent-to-Agent) 워크플로우 - MasterOrchestrator 기반

기존 Langgraph 워크플로우를 대체하여 순수 A2A 아키텍처로 동작합니다.
각 Agent의 결과에 따라 동적으로 플로우가 결정됩니다.
"""

import sys
import os
from typing import List
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.dynamic_orchestrator import DynamicOrchestrator
from agents.schema_intelligence_agent import create_schema_intelligence_agent
from agents.query_architect_agent import create_query_architect_agent
from agents.data_investigator_agent import create_data_investigator_agent
from agents.communication_specialist_agent import create_communication_specialist_agent
from db.bigquery_client import bq_client
from rag.schema_embedder import schema_embedder
from rag.schema_retriever import schema_retriever


class A2AWorkflow:
    """완전 동적 A2A 워크플로우 관리 클래스"""
    
    def __init__(self):
        self.orchestrator = None
        self.initialized = False
    
    async def initialize(self):
        """완전 동적 시스템 초기화"""
        print("🔧 완전 동적 A2A 워크플로우 초기화 중...")
        
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
            
            # DynamicOrchestrator 및 Agent 초기화
            self.orchestrator = DynamicOrchestrator()
            
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
            print("🎉 완전 동적 A2A 워크플로우 초기화 완료!")
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
        
        print(f"🚀 완전 동적 A2A 처리 시작: '{user_query}'")
        print("-" * 60)
        
        try:
            # DynamicOrchestrator를 통한 완전 동적 A2A 실행
            result = await self.orchestrator.execute_dynamic_workflow(user_query)
            
            print(f"✅ 완전 동적 A2A 처리 완료! ({result.get('execution_time', 0):.2f}초)")
            print(f"🎛️ 완료 유형: {result.get('completion_type', 'unknown')}")
            print(f"📊 실행된 Agent: {len(result.get('executed_agents', []))}개")
            print(f"🔄 반복 횟수: {result.get('iterations', 0)}회")
            
            if result.get('iterations', 0) < 5:
                print("⚡ 효율적인 동적 실행 - 최소한의 Agent만 사용됨")
            
            return result
            
        except Exception as e:
            print(f"❌ 완전 동적 A2A 처리 실패: {str(e)}")
            raise
    
    def get_system_status(self) -> dict:
        """시스템 상태 조회"""
        if not self.initialized:
            return {"status": "not_initialized"}
        
        return self.orchestrator.get_system_status()
    
    def get_available_agents(self) -> List[str]:
        """등록된 Agent 목록 반환 (편의 메서드)"""
        if not self.initialized:
            return []
        
        return self.orchestrator.get_available_agents()
    
    async def shutdown(self):
        """시스템 종료"""
        if self.orchestrator:
            await self.orchestrator.shutdown()
        print("👋 A2A 워크플로우 종료 완료")


# 편의 함수들
async def create_a2a_workflow():
    """완전 동적 A2A 워크플로우 생성 및 초기화"""
    workflow = A2AWorkflow()
    success = await workflow.initialize()
    
    if not success:
        raise Exception("완전 동적 A2A 워크플로우 초기화 실패")
    
    return workflow


async def process_single_query(user_query: str) -> dict:
    """단일 쿼리 완전 동적 처리 (편의 함수)"""
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
        
        print("\n🚀 완전 동적 A2A SQL Generator 시작!")
        print("=" * 60)
        print("💡 완전 동적 특징: Agent 결과에 따라 실시간으로 다음 Agent 결정")
        print("   • 첫 Agent 실행 → 결과 분석 → 다음 필요 Agent 동적 선택")
        print("   • 불확실성 없음 → 탐색 Agent 완전 스킵")
        print("   • SQL 실행 성공 → 즉시 완료 (추가 Agent 실행 안함)")
        print("   • 실행 실패 → 필요한 개선 Agent만 동적 추가")
        print("   • 최소한의 Agent만 사용하여 최대 효율성 달성")
        print("=" * 60)
        
        try:
            while True:
                user_input = input("\n💬 SQL 생성 요청: ").strip()
                
                if user_input.lower() in ['quit', 'exit', '종료']:
                    print("👋 완전 동적 A2A 워크플로우를 종료합니다.")
                    break
                
                if not user_input:
                    print("⚠️ 입력이 비어있습니다.")
                    continue
                
                try:
                    result = await workflow.process_query(user_input)
                    
                    # 결과 출력
                    print("\n" + "=" * 60)
                    print("🎯 완전 동적 A2A 실행 결과:")
                    
                    if result.get("success"):
                        completion_type = result.get("completion_type", "unknown")
                        executed_agents = result.get("executed_agents", [])
                        iterations = result.get("iterations", 0)
                        
                        print(f"✅ 성공 ({completion_type} 완료)")
                        print(f"📊 실행된 Agent: {', '.join(executed_agents)}")
                        print(f"🔄 동적 반복: {iterations}회")
                        
                        if iterations <= 3:
                            print("⚡ 매우 효율적! 최소한의 Agent로 완료됨")
                        elif iterations <= 5:
                            print("✨ 효율적! 적절한 수의 Agent 사용됨")
                        
                        # 최종 결과 표시
                        final_result = result.get("final_result", {})
                        if final_result.get("sql_query"):
                            sql = final_result["sql_query"]
                            print(f"\n📋 생성된 SQL: {sql[:80]}{'...' if len(sql) > 80 else ''}")
                            
                            if final_result.get("execution_result", {}).get("success"):
                                rows = final_result["execution_result"].get("returned_rows", 0)
                                print(f"📊 실행 결과: {rows}개 행 반환")
                        
                        # 실행 히스토리 간단 표시
                        execution_history = result.get("execution_history", [])
                        if execution_history:
                            agent_flow = " → ".join([h["agent_name"] for h in execution_history])
                            print(f"🔄 Agent 플로우: {agent_flow}")
                    else:
                        print(f"❌ 실패: {result.get('error', 'Unknown error')}")
                        if result.get("executed_agents"):
                            print(f"📊 시도된 Agent: {', '.join(result['executed_agents'])}")
                    
                except Exception as e:
                    print(f"❌ 처리 중 오류: {str(e)}")
                
        finally:
            await workflow.shutdown()
    
    async def test_mode():
        """완전 동적 테스트 모드"""
        test_queries = [
            "users 테이블의 모든 데이터를 보여줘",
            "최근 일주일간 가장 많은 금액을 결제한 유저 상위 5명을 보여줘",
            "월별 카테고리별 매출 현황을 분석해줘"
        ]
        
        workflow = await create_a2a_workflow()
        
        try:
            total_agents_used = 0
            total_iterations = 0
            
            for i, query in enumerate(test_queries, 1):
                print(f"\n🧪 완전 동적 테스트 {i}: {query}")
                print("=" * 60)
                
                result = await workflow.process_query(query)
                
                if result.get("success"):
                    completion_type = result.get("completion_type", "unknown")
                    executed_agents = result.get("executed_agents", [])
                    iterations = result.get("iterations", 0)
                    
                    total_agents_used += len(executed_agents)
                    total_iterations += iterations
                    
                    print(f"✅ 성공 - {completion_type} 완료")
                    print(f"📊 사용된 Agent: {len(executed_agents)}개 ({', '.join(executed_agents)})")
                    print(f"🔄 동적 반복: {iterations}회")
                    
                    if iterations <= 3:
                        print("⚡ 매우 효율적인 동적 실행!")
                    
                    # Agent 플로우 표시
                    execution_history = result.get("execution_history", [])
                    if execution_history:
                        agent_flow = " → ".join([h["agent_name"] for h in execution_history])
                        print(f"🔄 실행 플로우: {agent_flow}")
                else:
                    print(f"❌ 실패: {result.get('error')}")
            
            # 전체 통계
            print(f"\n📊 완전 동적 실행 통계:")
            print(f"   평균 Agent 사용: {total_agents_used / len(test_queries):.1f}개")
            print(f"   평균 반복 횟수: {total_iterations / len(test_queries):.1f}회")
            print(f"   효율성: {'매우 우수' if total_iterations / len(test_queries) <= 3 else '우수' if total_iterations / len(test_queries) <= 5 else '보통'}")
                
        finally:
            await workflow.shutdown()
    
    # 실행 모드 선택
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        asyncio.run(test_mode())
    else:
        asyncio.run(interactive_mode())