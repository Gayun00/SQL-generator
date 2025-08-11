"""
SQL Generator Orchestrator Agent - A2A 구조의 메인 워크플로우 관리
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import asyncio

from .user_communicator_agent import UserCommunicatorAgent
from .schema_analyzer_agent import SchemaAnalyzerAgent  
from .sql_generator_agent import SQLGeneratorAgent
from .sql_executor_agent import SQLExecutorAgent


@dataclass
class OrchestratorState:
    """Orchestrator 상태 관리"""
    user_input: str = ""
    schema_info: List[Dict] = None
    sql_query: str = ""
    execution_result: Dict = None
    current_step: str = "init"
    error: Optional[str] = None
    
    def __post_init__(self):
        if self.schema_info is None:
            self.schema_info = []


class OrchestratorAgent:
    """SQL 생성 파이프라인을 조율하는 Orchestrator Agent"""
    
    def __init__(self):
        """Orchestrator Agent 초기화"""
        # 서브 에이전트 인스턴스 생성
        self.user_communicator = UserCommunicatorAgent()
        self.schema_analyzer = SchemaAnalyzerAgent()
        self.sql_generator = SQLGeneratorAgent()
        self.sql_executor = SQLExecutorAgent()
        
    async def process_request(self, user_input: str) -> Dict[str, Any]:
        """
        사용자 요청을 처리하는 메인 워크플로우
        
        Args:
            user_input: 사용자 자연어 입력
            
        Returns:
            전체 처리 결과
        """
        print("🚀 SQL Generator Orchestrator 시작")
        
        # 상태 초기화
        state = OrchestratorState(user_input=user_input)
        
        try:
            # 1. UserCommunicator로 자연어 텍스트 입력받기
            print("\n📝 Step 1: 사용자 입력 처리")
            state = await self._process_user_input(state)
            if state.error:
                return self._create_error_response(state.error)
            
            # 2. SchemaAnalyzer로 RAG 검색을 통한 관련 스키마 정보 찾기
            print("\n🔍 Step 2: 스키마 정보 검색") 
            state = await self._analyze_schema(state)
            if state.error:
                return self._create_error_response(state.error)
            
            # 3. SQLGenerator로 SQL 쿼리문 생성
            print("\n⚡ Step 3: SQL 쿼리 생성")
            state = await self._generate_sql(state)
            if state.error:
                return self._create_error_response(state.error)
            
            # 4. SQLExecutor로 SQL 쿼리문 실행 및 결과 반환
            print("\n📊 Step 4: SQL 쿼리 실행")
            state = await self._execute_sql(state)
            if state.error:
                return self._create_error_response(state.error)
                
            print("\n✅ SQL Generator 파이프라인 완료!")
            
            return {
                "success": True,
                "user_input": state.user_input,
                "schema_info": state.schema_info,
                "sql_query": state.sql_query,
                "execution_result": state.execution_result,
                "message": "SQL 쿼리 생성 및 실행이 성공적으로 완료되었습니다."
            }
            
        except Exception as e:
            error_msg = f"Orchestrator 처리 중 오류 발생: {str(e)}"
            print(f"❌ {error_msg}")
            return self._create_error_response(error_msg)
    
    async def _process_user_input(self, state: OrchestratorState) -> OrchestratorState:
        """사용자 입력 처리 단계"""
        try:
            state.current_step = "user_input"
            
            # UserCommunicator를 통한 입력 처리
            result = await self.user_communicator.process_input(state.user_input)
            
            if not result.get("success", False):
                state.error = f"사용자 입력 처리 실패: {result.get('error', '알 수 없는 오류')}"
                return state
                
            print(f"   ✅ 사용자 입력 처리 완료: {state.user_input}")
            return state
            
        except Exception as e:
            state.error = f"사용자 입력 처리 중 오류: {str(e)}"
            return state
    
    async def _analyze_schema(self, state: OrchestratorState) -> OrchestratorState:
        """스키마 분석 단계"""
        try:
            state.current_step = "schema_analysis"
            
            # SchemaAnalyzer를 통한 관련 스키마 검색
            schema_result = await self.schema_analyzer.analyze_query(state.user_input)
            
            if not schema_result.get("success", False):
                state.error = f"스키마 분석 실패: {schema_result.get('error', '알 수 없는 오류')}"
                return state
                
            state.schema_info = schema_result.get("schema_info", [])
            print(f"   ✅ 스키마 정보 검색 완료: {len(state.schema_info)}개 테이블")
            return state
            
        except Exception as e:
            state.error = f"스키마 분석 중 오류: {str(e)}"
            return state
    
    async def _generate_sql(self, state: OrchestratorState) -> OrchestratorState:
        """SQL 생성 단계"""
        try:
            state.current_step = "sql_generation"
            
            # SQLGenerator를 통한 SQL 쿼리 생성
            sql_result = await self.sql_generator.generate_sql(
                user_query=state.user_input,
                schema_info=state.schema_info
            )
            
            if not sql_result.get("success", False):
                state.error = f"SQL 생성 실패: {sql_result.get('error', '알 수 없는 오류')}"
                return state
                
            state.sql_query = sql_result.get("sql_query", "")
            print(f"   ✅ SQL 쿼리 생성 완료")
            print(f"   📋 생성된 쿼리: {state.sql_query[:100]}...")
            return state
            
        except Exception as e:
            state.error = f"SQL 생성 중 오류: {str(e)}"
            return state
    
    async def _execute_sql(self, state: OrchestratorState) -> OrchestratorState:
        """SQL 실행 단계"""
        try:
            state.current_step = "sql_execution"
            
            # SQLExecutor를 통한 SQL 실행
            execution_result = await self.sql_executor.execute_query(state.sql_query)
            
            if not execution_result.get("success", False):
                state.error = f"SQL 실행 실패: {execution_result.get('error', '알 수 없는 오류')}"
                return state
                
            state.execution_result = execution_result
            print(f"   ✅ SQL 실행 완료: {execution_result.get('returned_rows', 0)}개 결과")
            return state
            
        except Exception as e:
            state.error = f"SQL 실행 중 오류: {str(e)}"
            return state
    
    def _create_error_response(self, error_msg: str) -> Dict[str, Any]:
        """에러 응답 생성"""
        return {
            "success": False,
            "error": error_msg,
            "user_input": "",
            "schema_info": [],
            "sql_query": "",
            "execution_result": None,
            "message": "요청 처리 중 오류가 발생했습니다."
        }


# 전역 Orchestrator 인스턴스
orchestrator_agent = OrchestratorAgent()