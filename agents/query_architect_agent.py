"""
QueryArchitect Agent - SQL 설계 및 최적화 전문 Agent

기존 sql_generator 노드를 Agent로 변환하여 SQL 아키텍처 설계,
쿼리 최적화, 성능 튜닝에 특화된 지능형 Agent로 구현했습니다.
"""

from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import re

from .base_agent import BaseAgent, AgentMessage, MessageType, AgentConfig, create_agent_config
from rag.schema_retriever import schema_retriever
from db.bigquery_client import bq_client
from langchain.schema import HumanMessage, SystemMessage
import json

logger = logging.getLogger(__name__)

class QueryComplexity:
    """쿼리 복잡도 분류"""
    SIMPLE = "simple"           # 단순 SELECT
    MODERATE = "moderate"       # JOIN, GROUP BY 포함
    COMPLEX = "complex"         # 서브쿼리, 윈도우 함수 등
    ADVANCED = "advanced"       # 복잡한 분석 쿼리

class QueryArchitectAgent(BaseAgent):
    """SQL 설계 및 최적화 전문 Agent"""
    
    def __init__(self, config: Optional[AgentConfig] = None):
        if config is None:
            config = create_agent_config(
                name="query_architect",
                specialization="sql_design_optimization",
                model="gpt-4",
                temperature=0.1,  # 정확성과 일관성 중시
                max_tokens=1500
            )
        
        super().__init__(config)
        
        # SQL 생성 전용 설정
        self.optimization_strategies = {
            "index_hints": "인덱스 활용 최적화",
            "join_optimization": "JOIN 순서 최적화", 
            "subquery_optimization": "서브쿼리 최적화",
            "window_function": "윈도우 함수 활용"
        }
        
        # 성능 추적
        self.generation_history = []
        self.performance_stats = {
            "simple_queries": 0,
            "complex_queries": 0,
            "optimization_applied": 0,
            "avg_generation_time": 0.0
        }
        
        logger.info(f"QueryArchitect Agent initialized with specialization: {self.specialization}")
    
    def get_system_prompt(self) -> str:
        """SQL 생성 전문 시스템 프롬프트"""
        return f"""
        당신은 SQL 설계 및 최적화 전문 AI Agent입니다.
        
        **전문 분야:**
        - BigQuery SQL 아키텍처 설계
        - 쿼리 성능 최적화 및 튜닝
        - 복잡한 JOIN 전략 수립
        - 인덱스 활용 및 실행 계획 최적화
        
        **핵심 역할:**
        1. 사용자 요청을 정확한 SQL로 변환
        2. 성능 최적화된 쿼리 생성
        3. BigQuery 특화 문법 및 함수 활용
        4. 스키마 정보 기반 정확한 테이블/컬럼 매핑
        
        **SQL 생성 원칙:**
        - BigQuery 표준 SQL 문법 사용
        - 테이블명은 완전한 형식 (dataset.table) 사용
        - 효율적이고 성능이 좋은 쿼리 생성
        - 적절한 LIMIT 사용으로 결과 제한 (기본 100)
        - 날짜/시간 처리에 TIMESTAMP, DATE 함수 적극 활용
        - JOIN이 필요한 경우 최적화된 JOIN 조건 사용
        - 집계 및 윈도우 함수 적절히 활용
        
        **성능 최적화 전략:**
        - 인덱스 활용 가능한 WHERE 조건 우선 배치
        - 불필요한 컬럼 조회 최소화
        - JOIN 순서 최적화 (작은 테이블 먼저)
        - 서브쿼리보다 JOIN 선호
        - 파티션 컬럼 활용 (날짜 기반)
        
        **품질 보장:**
        - 생성 시간: 평균 1-3초 목표
        - 정확도: 98% 이상 목표
        - 성능: 최적화 적용률 80% 이상 목표
        """
    
    async def process_message(self, message: AgentMessage) -> AgentMessage:
        """메시지 처리 - SQL 생성 작업 수행"""
        try:
            # 입력 유효성 검증
            if not await self.validate_input(message):
                return self.create_error_message(message, ValueError("Invalid input message"))
            
            # 메시지 히스토리에 추가
            self.add_message_to_history(message)
            
            # 작업 타입에 따른 처리
            task_type = message.content.get("task_type", "optimized_generation")
            
            if task_type == "simple_generation":
                result = await self._simple_generation(message.content)
            elif task_type == "optimized_generation":
                result = await self._optimized_generation(message.content)
            elif task_type == "draft_generation":
                result = await self._draft_generation(message.content)
            elif task_type == "final_optimization":
                result = await self._final_optimization(message.content)
            elif task_type == "execute_with_improvements":
                result = await self._execute_with_improvements(message.content)
            else:
                result = await self._optimized_generation(message.content)  # 기본값
            
            # 성공 응답 생성
            return self.create_response_message(message, result)
            
        except Exception as e:
            logger.error(f"QueryArchitect Agent processing failed: {str(e)}")
            return self.create_error_message(message, e)
    
    async def _simple_generation(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """단순 SQL 생성 - 빠른 처리용"""
        query = input_data.get("query", "")
        context = input_data.get("context", {})
        
        logger.info(f"QueryArchitect: Simple generation for query: '{query[:50]}...'")
        
        # 기본 RAG 검색
        try:
            relevant_context = schema_retriever.create_context_summary(query, max_tables=3)
        except Exception as e:
            logger.warning(f"RAG search failed: {str(e)}")
            relevant_context = "스키마 정보를 가져올 수 없습니다."
        
        # 단순 SQL 생성 프롬프트
        user_message = f"""
        사용자 요청: {query}
        
        스키마 정보:
        {relevant_context}
        
        단순하고 직관적인 BigQuery SQL 쿼리를 생성해주세요.
        복잡한 최적화는 생략하고 기본적인 기능만 구현하세요.
        
        SQL 쿼리만 반환하세요.
        """
        
        try:
            start_time = datetime.now()
            response_content = await self.send_llm_request(user_message)
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # SQL 정리
            sql_query = self._clean_sql_response(response_content)
            
            # 통계 업데이트
            self.performance_stats["simple_queries"] += 1
            self._update_generation_stats(processing_time)
            
            result = {
                "generation_type": "simple_generation",
                "sql_query": sql_query,
                "processing_time": processing_time,
                "complexity": QueryComplexity.SIMPLE,
                "optimization_applied": False,
                "schema_context_used": relevant_context is not None
            }
            
            # 생성 히스토리에 추가
            self._add_to_generation_history(query, result)
            
            logger.info(f"Simple generation completed in {processing_time:.2f}s")
            return result
            
        except Exception as e:
            logger.error(f"Simple generation failed: {str(e)}")
            return self._create_fallback_result("simple_generation", str(e))
    
    async def _optimized_generation(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """최적화된 SQL 생성 - 표준 처리"""
        query = input_data.get("query", "")
        analysis_result = input_data.get("analysis_result", {})
        exploration_result = input_data.get("exploration_result", {})
        
        logger.info(f"QueryArchitect: Optimized generation started")
        
        # 확장된 RAG 검색
        try:
            relevant_context = schema_retriever.create_context_summary(query, max_tables=5)
        except Exception as e:
            logger.warning(f"Extended RAG search failed: {str(e)}")
            relevant_context = "스키마 정보를 가져올 수 없습니다."
        
        # 탐색 결과 컨텍스트 생성
        exploration_context = self._build_exploration_context(exploration_result)
        
        # 분석 결과 컨텍스트 생성
        analysis_context = self._build_analysis_context(analysis_result)
        
        # 최적화된 SQL 생성 프롬프트
        user_message = f"""
        사용자 요청: {query}
        
        스키마 정보:
        {relevant_context}
        
        {analysis_context}
        
        {exploration_context}
        
        위 정보를 종합하여 성능 최적화된 BigQuery SQL 쿼리를 생성해주세요.
        
        최적화 고려사항:
        1. 인덱스 활용 가능한 WHERE 조건 배치
        2. 적절한 JOIN 순서 (작은 테이블 먼저)
        3. 불필요한 컬럼 조회 최소화
        4. 파티션 컬럼 활용 (날짜 기반)
        5. LIMIT 사용으로 결과 제한
        
        SQL 쿼리만 반환하세요.
        """
        
        try:
            start_time = datetime.now()
            response_content = await self.send_llm_request(user_message)
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # SQL 정리 및 검증
            sql_query = self._clean_sql_response(response_content)
            complexity = self._assess_query_complexity(sql_query)
            optimizations = self._detect_applied_optimizations(sql_query)
            
            # 통계 업데이트
            if complexity in [QueryComplexity.COMPLEX, QueryComplexity.ADVANCED]:
                self.performance_stats["complex_queries"] += 1
            if optimizations:
                self.performance_stats["optimization_applied"] += 1
            
            self._update_generation_stats(processing_time)
            
            result = {
                "generation_type": "optimized_generation",
                "sql_query": sql_query,
                "processing_time": processing_time,
                "complexity": complexity,
                "optimization_applied": len(optimizations) > 0,
                "applied_optimizations": optimizations,
                "schema_context_used": relevant_context is not None,
                "exploration_used": bool(exploration_result),
                "confidence": self._calculate_confidence(sql_query, analysis_result)
            }
            
            # 생성 히스토리에 추가
            self._add_to_generation_history(query, result)
            
            logger.info(f"Optimized generation completed in {processing_time:.2f}s")
            return result
            
        except Exception as e:
            logger.error(f"Optimized generation failed: {str(e)}")
            return self._create_fallback_result("optimized_generation", str(e))
    
    async def _draft_generation(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """초안 생성 - 복잡한 쿼리용"""
        query = input_data.get("query", "")
        analysis_result = input_data.get("analysis_result", {})
        
        logger.info("QueryArchitect: Draft generation for complex query")
        
        # 다단계 접근법
        # 1단계: 기본 구조 생성
        basic_structure = await self._generate_basic_structure(query, analysis_result)
        
        # 2단계: JOIN 및 관계 추가
        with_relationships = await self._add_table_relationships(basic_structure, analysis_result)
        
        # 3단계: 조건 및 필터 추가
        complete_draft = await self._add_conditions_and_filters(with_relationships, query)
        
        result = {
            "generation_type": "draft_generation",
            "sql_query": complete_draft,
            "processing_stages": ["basic_structure", "relationships", "conditions"],
            "complexity": QueryComplexity.COMPLEX,
            "requires_review": True
        }
        
        return result
    
    async def _final_optimization(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """최종 최적화 - 검토 후 개선"""
        draft_sql = input_data.get("draft_sql", "")
        feedback = input_data.get("feedback", {})
        
        logger.info("QueryArchitect: Final optimization started")
        
        # 피드백 기반 개선
        optimization_prompt = f"""
        초안 SQL: {draft_sql}
        
        검토 피드백: {feedback}
        
        피드백을 반영하여 SQL을 최적화해주세요.
        특히 성능, 정확성, 가독성을 개선해주세요.
        
        최적화된 SQL 쿼리만 반환하세요.
        """
        
        try:
            response_content = await self.send_llm_request(optimization_prompt)
            optimized_sql = self._clean_sql_response(response_content)
            
            result = {
                "generation_type": "final_optimization",
                "sql_query": optimized_sql,
                "original_sql": draft_sql,
                "feedback_applied": feedback,
                "optimization_complete": True
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Final optimization failed: {str(e)}")
            return {"generation_type": "final_optimization", "sql_query": draft_sql, "error": str(e)}
    
    def _build_exploration_context(self, exploration_result: Dict) -> str:
        """탐색 결과를 컨텍스트로 변환"""
        if not exploration_result or not exploration_result.get("insights"):
            return ""
        
        insights = exploration_result.get("insights", [])
        return f"""
=== 탐색을 통해 발견된 정보 ===
{chr(10).join([f"- {insight}" for insight in insights])}

이 정보를 바탕으로 더 정확한 SQL 쿼리를 생성하세요.
        """
    
    def _build_analysis_context(self, analysis_result: Dict) -> str:
        """분석 결과를 컨텍스트로 변환"""
        if not analysis_result:
            return ""
        
        uncertainties = analysis_result.get("uncertainties", [])
        if not uncertainties:
            return ""
        
        context = "=== 불확실성 분석 결과 ===\n"
        for uncertainty in uncertainties:
            context += f"- {uncertainty.get('type', 'unknown')}: {uncertainty.get('description', 'N/A')}\n"
        
        context += "\n이러한 불확실성을 고려하여 적절한 가정을 세우고 SQL을 생성해주세요.\n"
        return context
    
    def _clean_sql_response(self, response_content: str) -> str:
        """SQL 응답 정리"""
        sql_query = response_content.strip()
        
        # 코드 블록 제거
        if sql_query.startswith("```sql"):
            sql_query = sql_query[6:]
        if sql_query.startswith("```"):
            sql_query = sql_query[3:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
        
        return sql_query.strip()
    
    def _assess_query_complexity(self, sql_query: str) -> str:
        """쿼리 복잡도 평가"""
        sql_lower = sql_query.lower()
        
        # 고급 기능 검출
        advanced_patterns = ["window", "partition by", "row_number", "rank", "cte", "with recursive"]
        if any(pattern in sql_lower for pattern in advanced_patterns):
            return QueryComplexity.ADVANCED
        
        # 복잡한 기능 검출
        complex_patterns = ["subquery", "exists", "case when", "union", "having"]
        subquery_count = len(re.findall(r'\bselect\b', sql_lower)) - 1
        if subquery_count > 0 or any(pattern in sql_lower for pattern in complex_patterns):
            return QueryComplexity.COMPLEX
        
        # 중간 복잡도 검출
        moderate_patterns = ["join", "group by", "order by", "distinct"]
        if any(pattern in sql_lower for pattern in moderate_patterns):
            return QueryComplexity.MODERATE
        
        return QueryComplexity.SIMPLE
    
    def _detect_applied_optimizations(self, sql_query: str) -> List[str]:
        """적용된 최적화 기법 탐지"""
        optimizations = []
        sql_lower = sql_query.lower()
        
        if "limit" in sql_lower:
            optimizations.append("result_limiting")
        
        if "where" in sql_lower and ("=" in sql_lower or "in" in sql_lower):
            optimizations.append("indexed_filtering")
        
        if "join" in sql_lower:
            optimizations.append("join_optimization")
        
        if any(pattern in sql_lower for pattern in ["timestamp", "date", "datetime"]):
            optimizations.append("temporal_optimization")
        
        return optimizations
    
    def _calculate_confidence(self, sql_query: str, analysis_result: Dict) -> float:
        """생성된 SQL의 신뢰도 계산"""
        confidence = 0.8  # 기본 신뢰도
        
        # 스키마 정보 활용 여부
        if bq_client.schema_info:
            confidence += 0.1
        
        # 불확실성 해결 여부
        uncertainties = analysis_result.get("uncertainties", []) if analysis_result else []
        if not uncertainties:
            confidence += 0.1
        elif len(uncertainties) > 3:
            confidence -= 0.2
        
        # SQL 복잡도에 따른 조정
        complexity = self._assess_query_complexity(sql_query)
        if complexity == QueryComplexity.SIMPLE:
            confidence += 0.05
        elif complexity == QueryComplexity.ADVANCED:
            confidence -= 0.1
        
        return min(max(confidence, 0.0), 1.0)
    
    async def _generate_basic_structure(self, query: str, analysis: Dict) -> str:
        """기본 SQL 구조 생성"""
        prompt = f"""
        사용자 요청: {query}
        
        기본적인 SELECT 구조만 생성해주세요.
        복잡한 JOIN이나 조건은 제외하고 핵심 테이블과 컬럼만 포함하세요.
        
        SQL 쿼리만 반환하세요.
        """
        
        response = await self.send_llm_request(prompt)
        return self._clean_sql_response(response)
    
    async def _add_table_relationships(self, basic_sql: str, analysis: Dict) -> str:
        """테이블 관계 추가"""
        prompt = f"""
        기본 SQL: {basic_sql}
        
        필요한 JOIN을 추가해주세요.
        테이블 간 관계를 정확히 파악하여 적절한 JOIN 조건을 설정하세요.
        
        개선된 SQL 쿼리만 반환하세요.
        """
        
        response = await self.send_llm_request(prompt)
        return self._clean_sql_response(response)
    
    async def _add_conditions_and_filters(self, sql_with_joins: str, original_query: str) -> str:
        """조건 및 필터 추가"""
        prompt = f"""
        현재 SQL: {sql_with_joins}
        원본 요청: {original_query}
        
        사용자 요청에 맞는 WHERE 조건, ORDER BY, LIMIT 등을 추가해주세요.
        
        완성된 SQL 쿼리만 반환하세요.
        """
        
        response = await self.send_llm_request(prompt)
        return self._clean_sql_response(response)
    
    async def _execute_with_improvements(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """SQL 실행 및 실패시 개선방안 즉시 적용"""
        sql_query = input_data.get("sql_query", "")
        original_query = input_data.get("original_query", "")
        
        logger.info("QueryArchitect: Execute with improvements started")
        
        if not sql_query:
            return {
                "execution_type": "execute_with_improvements",
                "success": False,
                "error": "실행할 SQL 쿼리가 없습니다.",
                "sql_query": sql_query
            }
        
        start_time = datetime.now()
        
        # 1단계: 원본 SQL 실행 시도
        print(f"🔄 SQL 실행 중...")
        print(f"📝 SQL: {sql_query}")
        
        try:
            query_result = bq_client.execute_query(sql_query)
            processing_time = (datetime.now() - start_time).total_seconds()
            
            if query_result["success"]:
                # 성공시 바로 반환
                print(f"✅ SQL 실행 성공! ({processing_time:.2f}초)")
                print(f"📊 결과: {query_result['returned_rows']}개 행 반환")
                
                return {
                    "execution_type": "execute_with_improvements",
                    "success": True,
                    "sql_query": sql_query,
                    "query_result": query_result,
                    "processing_time": processing_time,
                    "improvements_applied": False
                }
            
            # 2단계: 실패시 개선방안 생성
            print(f"❌ SQL 실행 실패: {query_result.get('error', 'Unknown error')}")
            print("🔧 개선방안 생성 중...")
            
            improvements = await self._generate_sql_improvements(sql_query, query_result.get('error', ''), original_query)
            
            if not improvements:
                return {
                    "execution_type": "execute_with_improvements", 
                    "success": False,
                    "sql_query": sql_query,
                    "error": query_result.get('error', ''),
                    "improvements_generated": False
                }
            
            # 3단계: 개선방안 출력 및 사용자 확인
            print("\n🛠️ 제안된 개선방안:")
            for i, improvement in enumerate(improvements, 1):
                print(f"{i}. {improvement['description']}")
                if improvement.get('improved_sql'):
                    print(f"   개선된 SQL: {improvement['improved_sql'][:100]}...")
            
            # 4단계: 자동 실행 (가장 신뢰도 높은 개선안)
            best_improvement = max(improvements, key=lambda x: x.get('confidence', 0))
            
            if best_improvement.get('confidence', 0) > 0.7:
                print(f"\n🚀 신뢰도 높은 개선안을 자동 실행합니다. (신뢰도: {best_improvement['confidence']:.2f})")
                return await self._execute_improved_sql(best_improvement, start_time)
            else:
                # 신뢰도가 낮으면 사용자 확인 요청
                if await self._ask_user_confirmation_async():
                    return await self._execute_improved_sql(best_improvement, start_time)
                else:
                    print("❌ 사용자가 개선방안 실행을 취소했습니다.")
                    return {
                        "execution_type": "execute_with_improvements",
                        "success": False,
                        "sql_query": sql_query,
                        "error": query_result.get('error', ''),
                        "improvements_generated": True,
                        "improvements_applied": False,
                        "user_cancelled": True
                    }
            
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Execute with improvements failed: {str(e)}")
            return {
                "execution_type": "execute_with_improvements",
                "success": False,
                "sql_query": sql_query,
                "error": str(e),
                "processing_time": processing_time
            }
    
    async def _generate_sql_improvements(self, sql_query: str, error_message: str, original_query: str) -> List[Dict[str, Any]]:
        """SQL 오류 분석 및 개선방안 생성"""
        
        # 스키마 정보 준비
        schema_context = self._build_schema_context_for_improvement(sql_query)
        
        system_prompt = f"""
        당신은 BigQuery SQL 오류 분석 및 개선 전문가입니다.
        
        **분석할 정보:**
        - 원본 사용자 요청: {original_query}
        - 실패한 SQL: {sql_query}
        - 오류 메시지: {error_message}
        
        **스키마 정보:**
        {schema_context}
        
        **개선 전략:**
        1. 컬럼명 오류: 정확한 컬럼명으로 수정 (오류 메시지의 "Did you mean" 활용)
        2. 데이터 타입 오류: PARSE_TIMESTAMP, CAST 등 적절한 타입 변환
        3. 테이블명 오류: 올바른 dataset.table 형식으로 수정
        4. 문법 오류: BigQuery 표준 SQL 문법 준수
        5. 함수 사용 오류: 올바른 함수 및 파라미터
        
        **응답 형식 (JSON):**
        {{
            "improvements": [
                {{
                    "issue_type": "column_name|data_type|table_name|syntax|function",
                    "description": "구체적인 문제점과 해결책 설명",
                    "improved_sql": "완전히 수정된 SQL 쿼리",
                    "confidence": 0.0-1.0,
                    "changes_made": ["변경사항1", "변경사항2"]
                }}
            ]
        }}
        """
        
        try:
            response_content = await self.send_llm_request(system_prompt)
            
            # JSON 파싱
            content = response_content.strip()
            if content.startswith("```json"):
                content = content[7:]
            if content.startswith("```"):
                content = content[3:]
            if content.endswith("```"):
                content = content[:-3]
            
            improvement_data = json.loads(content.strip())
            improvements = improvement_data.get("improvements", [])
            
            # 기본 개선방안 추가 (AI가 놓친 부분 보완)
            basic_improvements = self._generate_basic_improvements(sql_query, error_message)
            improvements.extend(basic_improvements)
            
            return improvements
            
        except Exception as e:
            logger.error(f"AI improvement generation failed: {str(e)}")
            # AI 실패시 기본 개선방안만 반환
            return self._generate_basic_improvements(sql_query, error_message)
    
    def _generate_basic_improvements(self, sql_query: str, error_message: str) -> List[Dict[str, Any]]:
        """기본적인 개선방안 생성 (패턴 기반)"""
        improvements = []
        
        # 1. 컬럼명 오류 처리
        if "Unrecognized name" in error_message:
            match = re.search(r"Unrecognized name: (\w+)", error_message)
            suggestion_match = re.search(r"Did you mean (\w+)?", error_message)
            
            if match and suggestion_match:
                wrong_column = match.group(1)
                correct_column = suggestion_match.group(1)
                improved_sql = sql_query.replace(wrong_column, correct_column)
                
                improvements.append({
                    "issue_type": "column_name",
                    "description": f"컬럼명 '{wrong_column}'을 '{correct_column}'으로 수정",
                    "improved_sql": improved_sql,
                    "confidence": 0.95,
                    "changes_made": [f"{wrong_column} → {correct_column}"]
                })
        
        # 2. 데이터 타입 오류 처리 
        elif "No matching signature" in error_message and ("TIMESTAMP" in error_message or "STRING" in error_message):
            if "createdAt" in sql_query:
                # createdAt 컬럼을 TIMESTAMP로 변환 (ISO 8601 형식)
                improved_sql = re.sub(
                    r"(\w+\.)?createdAt(\s*[><=]+\s*)",
                    r"PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', \1createdAt)\2",
                    sql_query
                )
                
                improvements.append({
                    "issue_type": "data_type",
                    "description": "createdAt 컬럼을 ISO 8601 형식의 PARSE_TIMESTAMP로 변환하여 날짜 비교 가능하도록 수정",
                    "improved_sql": improved_sql,
                    "confidence": 0.9,
                    "changes_made": ["createdAt → PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', createdAt)"]
                })
        
        # 3. 함수 오류 처리
        elif "CURRENT_DATE" in sql_query and "INTERVAL" in error_message:
            # CURRENT_DATE() → CURRENT_TIMESTAMP()로 수정
            improved_sql = sql_query.replace("CURRENT_DATE()", "CURRENT_TIMESTAMP()")
            
            improvements.append({
                "issue_type": "function",
                "description": "날짜 함수를 CURRENT_TIMESTAMP()로 수정",
                "improved_sql": improved_sql,
                "confidence": 0.8,
                "changes_made": ["CURRENT_DATE() → CURRENT_TIMESTAMP()"]
            })
        
        return improvements
    
    def _build_schema_context_for_improvement(self, sql_query: str) -> str:
        """개선을 위한 스키마 컨텍스트 생성"""
        try:
            schema_info = getattr(bq_client, 'schema_info', [])
            if not schema_info:
                return "스키마 정보가 없습니다."
            
            # SQL에서 언급된 테이블 찾기
            mentioned_tables = []
            for table_info in schema_info:
                # schema_info가 문자열인 경우 처리
                if isinstance(table_info, str):
                    continue
                    
                table_name = table_info.get("table_name", "") if isinstance(table_info, dict) else ""
                if table_name and table_name.lower() in sql_query.lower():
                    mentioned_tables.append(table_info)
            
            if not mentioned_tables:
                return "관련 테이블을 찾을 수 없습니다."
            
            context = "관련 테이블 스키마:\n"
            for table in mentioned_tables[:2]:  # 최대 2개
                if not isinstance(table, dict):
                    continue
                    
                table_name = table.get("table_name", "")
                columns = table.get("columns", [])
                
                context += f"\n테이블: {table_name}\n"
                for col in columns[:8]:  # 최대 8개 컬럼
                    if isinstance(col, dict):
                        col_name = col.get("column_name", "")
                        col_type = col.get("data_type", "")
                        context += f"  - {col_name} ({col_type})\n"
            
            return context
            
        except Exception as e:
            logger.error(f"Schema context building failed: {str(e)}")
            return "스키마 정보 처리 중 오류가 발생했습니다."
    
    async def _ask_user_confirmation_async(self) -> bool:
        """사용자 확인 (비동기)"""
        try:
            print("\n❓ 개선된 쿼리를 실행하시겠습니까?")
            print("   y/yes - 실행")
            print("   n/no - 취소")
            
            # 실제 프로덕션에서는 적절한 비동기 입력 처리 필요
            # 현재는 간단한 동기 처리
            import asyncio
            
            def get_input():
                return input("\n선택하세요 (y/n): ").strip().lower()
            
            response = await asyncio.get_event_loop().run_in_executor(None, get_input)
            return response in ['y', 'yes', '예', '네']
            
        except Exception as e:
            logger.error(f"User confirmation failed: {str(e)}")
            return False
    
    async def _execute_improved_sql(self, improvement: Dict, start_time: datetime) -> Dict[str, Any]:
        """개선된 SQL 실행"""
        improved_sql = improvement.get('improved_sql', '')
        
        if not improved_sql:
            return {
                "execution_type": "execute_with_improvements",
                "success": False,
                "error": "개선된 SQL이 없습니다."
            }
        
        print(f"\n🔄 개선된 쿼리 실행 중...")
        print(f"📝 개선된 SQL: {improved_sql}")
        print(f"🛠️ 적용된 개선사항: {improvement.get('description', '')}")
        
        try:
            query_result = bq_client.execute_query(improved_sql)
            processing_time = (datetime.now() - start_time).total_seconds()
            
            if query_result["success"]:
                print(f"✅ 개선된 쿼리 실행 성공! ({processing_time:.2f}초)")
                print(f"📊 결과: {query_result['returned_rows']}개 행 반환")
                
                # 성공 통계 업데이트
                self.performance_stats["optimization_applied"] += 1
                
                return {
                    "execution_type": "execute_with_improvements",
                    "success": True,
                    "sql_query": improved_sql,
                    "original_sql": improvement.get('original_sql', ''),
                    "query_result": query_result,
                    "processing_time": processing_time,
                    "improvements_applied": True,
                    "improvement_details": {
                        "type": improvement.get('issue_type', ''),
                        "description": improvement.get('description', ''),
                        "confidence": improvement.get('confidence', 0),
                        "changes_made": improvement.get('changes_made', [])
                    }
                }
            else:
                print(f"❌ 개선된 쿼리도 실행 실패: {query_result.get('error', '')}")
                return {
                    "execution_type": "execute_with_improvements",
                    "success": False,
                    "sql_query": improved_sql,
                    "error": query_result.get('error', ''),
                    "processing_time": processing_time,
                    "improvements_applied": True,
                    "improvement_failed": True
                }
                
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Improved SQL execution failed: {str(e)}")
            return {
                "execution_type": "execute_with_improvements",
                "success": False,
                "sql_query": improved_sql,
                "error": str(e),
                "processing_time": processing_time
            }
    
    def _update_generation_stats(self, processing_time: float):
        """생성 통계 업데이트"""
        total_queries = self.performance_stats["simple_queries"] + self.performance_stats["complex_queries"]
        if total_queries > 0:
            current_avg = self.performance_stats["avg_generation_time"]
            new_avg = ((current_avg * (total_queries - 1)) + processing_time) / total_queries
            self.performance_stats["avg_generation_time"] = new_avg
    
    def _add_to_generation_history(self, query: str, result: Dict):
        """생성 히스토리에 추가"""
        self.generation_history.append({
            "timestamp": datetime.now().isoformat(),
            "query": query,
            "generation_type": result.get("generation_type"),
            "complexity": result.get("complexity"),
            "processing_time": result.get("processing_time", 0),
            "optimization_applied": result.get("optimization_applied", False)
        })
        
        # 히스토리 크기 제한
        if len(self.generation_history) > 50:
            self.generation_history = self.generation_history[-50:]
    
    def _create_fallback_result(self, generation_type: str, error_msg: str) -> Dict[str, Any]:
        """생성 실패시 대체 결과 생성"""
        return {
            "generation_type": generation_type,
            "sql_query": "-- SQL 생성 실패로 인한 기본 쿼리\nSELECT 'ERROR' as message;",
            "error": error_msg,
            "fallback": True,
            "confidence": 0.0
        }
    
    def get_agent_statistics(self) -> Dict[str, Any]:
        """Agent 통계 정보 반환"""
        total_queries = self.performance_stats["simple_queries"] + self.performance_stats["complex_queries"]
        
        if total_queries == 0:
            return {"message": "생성 이력이 없습니다."}
        
        optimization_rate = (self.performance_stats["optimization_applied"] / total_queries) * 100
        
        return {
            "total_generated": total_queries,
            "simple_queries": self.performance_stats["simple_queries"],
            "complex_queries": self.performance_stats["complex_queries"],
            "optimization_rate": round(optimization_rate, 2),
            "avg_generation_time": round(self.performance_stats["avg_generation_time"], 3),
            "performance_grade": "A" if optimization_rate > 70 and self.performance_stats["avg_generation_time"] < 2.0 else "B"
        }

# Agent 생성 헬퍼 함수
def create_query_architect_agent(custom_config: Optional[Dict[str, Any]] = None) -> QueryArchitectAgent:
    """QueryArchitect Agent 생성"""
    config = create_agent_config(
        name="query_architect",
        specialization="sql_design_optimization",
        model="gpt-4",
        temperature=0.1,
        max_tokens=1500,
        **(custom_config or {})
    )
    
    return QueryArchitectAgent(config)