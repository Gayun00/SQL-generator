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
from agents.schema_analyzer_agent import create_schema_analyzer_agent
from agents.sql_generator_agent import create_sql_generator_agent
from agents.data_explorer_agent import create_data_explorer_agent
from agents.user_communicator_agent import create_user_communicator_agent
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
                create_schema_analyzer_agent(),
                create_sql_generator_agent(),
                create_data_explorer_agent(),
                create_user_communicator_agent()
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
            print(f"🎛️ 완료 유형: {result.get('termination_reason', 'unknown')}")
            print(f"📊 실행된 Agent: {len(result.get('executed_agents', []))}개")
            print(f"🔄 반복 횟수: {result.get('iterations', 0)}회")
            
            if result.get('iterations', 0) < 5:
                print("⚡ 효율적인 동적 실행 - 최소한의 Agent만 사용됨")
            
            return result
            
        except Exception as e:
            print(f"❌ 완전 동적 A2A 처리 실패: {str(e)}")
            raise
    
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
