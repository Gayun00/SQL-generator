"""
SQL Generator Agent - 자연어 쿼리를 SQL로 변환
"""

from typing import Dict, Any, List, Optional, TypedDict
import re
import json
from langgraph.graph import StateGraph, END
from langgraph.graph.state import CompiledStateGraph
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage


class SQLGeneratorInternalState(TypedDict):
    """SQL Generator 내부 상태 관리"""
    user_query: str
    schema_info: List[Dict]
    current_sql: str
    query_analysis: Optional[Dict]
    user_feedback: Optional[str]
    user_choice: Optional[str]  # "execute" or "modify"
    iteration_count: int
    modification_history: List[Dict]


class SQLGeneratorAgent:
    """자연어 쿼리를 기반으로 SQL을 생성하는 에이전트"""
    
    def __init__(self):
        """SQLGenerator Agent 초기화"""
        print("⚡ SQLGenerator Agent 초기화")
        self.llm = ChatOpenAI(
            model="gpt-4o-mini",
            temperature=0.1,
            max_tokens=2000
        )
        self.workflow: Optional[CompiledStateGraph] = None
        self._build_workflow()
    
    def _build_workflow(self):
        """LangGraph 워크플로우 구성"""
        graph = StateGraph(SQLGeneratorInternalState)
        
        # 노드 추가
        graph.add_node("generate_sql_node", self._generate_sql_node)
        graph.add_node("human_review_node", self._human_review_node)
        graph.add_node("modify_sql_node", self._modify_sql_node)
        
        # 시작점 설정
        graph.set_entry_point("generate_sql_node")
        
        # 엣지 설정
        graph.add_edge("generate_sql_node", "human_review_node")
        graph.add_edge("modify_sql_node", "human_review_node")
        
        # 조건부 엣지 설정
        graph.add_conditional_edges(
            "human_review_node",
            self._route_user_decision,
            {
                "execute": END,
                "modify": "modify_sql_node"
            }
        )
        
        # 컴파일
        self.workflow = graph.compile()
        print("🔧 SQL Generator LangGraph 워크플로우 구성 완료")

    async def generate_sql(self, user_query: str, schema_info: List[Dict]) -> Dict[str, Any]:
        """
        Human-in-the-loop을 포함한 SQL 생성 (LangGraph 워크플로우 사용)
        
        Args:
            user_query: 사용자 자연어 쿼리
            schema_info: 관련 스키마 정보
            
        Returns:
            최종 SQL 생성 결과
        """
        try:
            print(f"⚡ Human-in-the-Loop SQL 생성 시작: {user_query}")
            
            # 스키마 정보 검증
            if not schema_info:
                return {
                    "success": False,
                    "error": "관련 스키마 정보가 없습니다. 다른 키워드로 시도해보세요.",
                    "sql_query": ""
                }
            
            # 초기 상태 설정
            initial_state = SQLGeneratorInternalState(
                user_query=user_query,
                schema_info=schema_info,
                current_sql="",
                query_analysis=None,
                user_feedback=None,
                user_choice=None,
                iteration_count=0,
                modification_history=[]
            )
            
            # LangGraph 워크플로우 실행
            final_state = await self.workflow.ainvoke(initial_state)
            
            # 최종 결과 검증
            final_sql = final_state.get("current_sql", "")
            if not final_sql or final_sql == "SELECT 1 as error_query":
                return {
                    "success": False,
                    "error": "SQL 쿼리 생성에 실패했습니다.",
                    "sql_query": ""
                }
            
            # SQL 검증
            validation_result = self._validate_sql(final_sql)
            
            print(f"✅ Human-in-the-Loop SQL 생성 완료!")
            
            return {
                "success": True,
                "sql_query": final_sql,
                "query_analysis": final_state.get("query_analysis"),
                "schema_info": schema_info,
                "validation": validation_result,
                "iteration_count": final_state.get("iteration_count", 0),
                "modification_history": final_state.get("modification_history", []),
                "message": "SQL 쿼리가 성공적으로 생성되었습니다."
            }
            
        except Exception as e:
            error_msg = f"SQL 생성 중 오류: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                "success": False,
                "error": error_msg,
                "sql_query": ""
            }
    
    async def _generate_sql_node(self, state: SQLGeneratorInternalState) -> SQLGeneratorInternalState:
        """SQL 생성 노드"""
        try:
            print(f"⚡ SQL 생성 중... (반복: {state['iteration_count'] + 1})")
            
            # 쿼리 분석 (첫 번째 반복에서만)
            if state["iteration_count"] == 0:
                state["query_analysis"] = self._analyze_query(state["user_query"])
            
            # SQL 생성
            sql_query = self._generate_sql_query(
                state["user_query"], 
                state["schema_info"], 
                state["query_analysis"]
            )
            
            if not sql_query:
                raise Exception("SQL 쿼리 생성에 실패했습니다.")
            
            state["current_sql"] = sql_query
            state["iteration_count"] += 1
            
            print(f"✅ SQL 생성 완료")
            return state
            
        except Exception as e:
            print(f"❌ SQL 생성 오류: {str(e)}")
            # 오류 발생 시 기본 쿼리라도 생성
            state["current_sql"] = "SELECT 1 as error_query"
            return state
    
    async def _human_review_node(self, state: SQLGeneratorInternalState) -> SQLGeneratorInternalState:
        """사용자 검토 노드 - SQL 표시 및 사용자 선택 입력"""
        try:
            print("\n" + "="*60)
            print("📋 생성된 SQL 쿼리:")
            print("="*60)
            print(state["current_sql"])
            print("="*60)
            
            # 사용자 선택 입력
            print("\n🤔 이 SQL을 실행하시겠습니까?")
            print("1. 실행")
            print("2. 수정")
            
            choice = input("선택 (1 또는 2): ").strip()
            
            if choice == "2":
                print("\n✏️ 어떻게 수정하시겠습니까?")
                feedback = input("수정 요청사항: ").strip()
                
                if not feedback:
                    print("⚠️ 수정 요청사항이 없습니다. 실행으로 처리합니다.")
                    choice = "1"
                else:
                    state["user_feedback"] = feedback
                    # 수정 이력 저장
                    state["modification_history"].append({
                        "iteration": state["iteration_count"],
                        "original_sql": state["current_sql"],
                        "feedback": feedback
                    })
            
            state["user_choice"] = "execute" if choice == "1" else "modify"
            
            print(f"👤 사용자 선택: {'실행' if choice == '1' else '수정'}")
            return state
            
        except Exception as e:
            print(f"❌ 사용자 입력 처리 오류: {str(e)}")
            # 오류 시 기본적으로 실행 선택
            state["user_choice"] = "execute"
            return state
    
    def _route_user_decision(self, state: SQLGeneratorInternalState) -> str:
        """사용자 선택에 따른 라우팅"""
        return state.get("user_choice", "execute")
    
    async def _modify_sql_node(self, state: SQLGeneratorInternalState) -> SQLGeneratorInternalState:
        """SQL 수정 노드 - LLM이 사용자 피드백을 기반으로 SQL 수정"""
        try:
            print(f"🔧 SQL 수정 중: {state['user_feedback']}")
            
            # LLM을 통한 SQL 수정
            modified_sql = await self._modify_sql_with_llm(
                current_sql=state["current_sql"],
                user_feedback=state["user_feedback"],
                original_query=state["user_query"],
                schema_info=state["schema_info"]
            )
            
            if modified_sql and modified_sql != state["current_sql"]:
                state["current_sql"] = modified_sql
                print("✅ SQL 수정 완료")
            else:
                print("⚠️ 수정사항이 없거나 수정 실패, 기존 SQL 유지")
            
            # 피드백 초기화
            state["user_feedback"] = None
            
            return state
            
        except Exception as e:
            print(f"❌ SQL 수정 오류: {str(e)}")
            return state
    
    async def _modify_sql_with_llm(self, current_sql: str, user_feedback: str, original_query: str, schema_info: List[Dict]) -> str:
        """LLM을 사용하여 사용자 피드백 기반 SQL 수정"""
        try:
            # 스키마 정보를 문자열로 변환
            schema_context = self._format_schema_for_llm(schema_info)
            
            # LLM 프롬프트 구성
            system_message = SystemMessage(content="""
당신은 BigQuery SQL 전문가입니다. 사용자의 피드백을 바탕으로 기존 SQL 쿼리를 정확하게 수정해주세요.

지시사항:
1. 사용자의 수정 요청을 정확히 분석하고 반영하세요
2. BigQuery 문법을 사용하세요 (테이블명은 백틱으로 감싸기)
3. 수정된 완전한 SQL 쿼리만 반환하세요
4. SQL 주석이나 설명은 포함하지 마세요
5. SQL 문법이 올바른지 확인하세요
""")
            
            human_message = HumanMessage(content=f"""
**원본 사용자 요청:**
{original_query}

**현재 SQL 쿼리:**
```sql
{current_sql}
```

**사용자 수정 요청:**
{user_feedback}

**사용 가능한 스키마 정보:**
{schema_context}

위 정보를 바탕으로 사용자의 수정 요청을 반영한 SQL 쿼리를 작성해주세요.
""")
            
            # LLM 호출
            print("🤖 LLM을 통한 SQL 수정 진행 중...")
            response = await self.llm.ainvoke([system_message, human_message])
            
            # 응답에서 SQL 추출
            modified_sql = response.content.strip()
            
            # 코드 블록 제거 (```sql과 ``` 제거)
            if modified_sql.startswith("```sql"):
                modified_sql = modified_sql.replace("```sql", "").replace("```", "").strip()
            elif modified_sql.startswith("```"):
                modified_sql = modified_sql.replace("```", "").strip()
            
            print("✅ LLM SQL 수정 완료")
            return modified_sql
            
        except Exception as e:
            print(f"❌ LLM SQL 수정 중 오류: {str(e)}")
            return current_sql
    
    def _format_schema_for_llm(self, schema_info: List[Dict]) -> str:
        """스키마 정보를 LLM이 이해하기 쉬운 형태로 포맷팅"""
        if not schema_info:
            return "스키마 정보가 없습니다."
        
        schema_text = []
        for table in schema_info:
            table_name = table.get("table_name", "")
            description = table.get("description", "")
            columns = table.get("columns", [])
            
            schema_text.append(f"테이블: {table_name}")
            if description:
                schema_text.append(f"  설명: {description}")
            
            schema_text.append("  컬럼:")
            for col in columns:
                col_name = col.get("name", "")
                col_type = col.get("type", "")
                col_desc = col.get("description", "")
                col_line = f"    - {col_name} ({col_type})"
                if col_desc:
                    col_line += f": {col_desc}"
                schema_text.append(col_line)
            schema_text.append("")
        
        return "\n".join(schema_text)
    
    def _analyze_query(self, user_query: str) -> Dict[str, Any]:
        """사용자 쿼리 분석"""
        analysis = {
            "intent": "select",  # select, count, sum, avg, etc.
            "conditions": [],
            "aggregations": [],
            "time_filters": [],
            "order_by": None,
            "limit": None
        }
        
        query_lower = user_query.lower()
        
        # 의도 분석
        if any(word in query_lower for word in ['개수', '수량', '몇 개', 'count']):
            analysis["intent"] = "count"
        elif any(word in query_lower for word in ['합계', '총합', '총액', 'sum']):
            analysis["intent"] = "sum"
        elif any(word in query_lower for word in ['평균', 'avg', 'average']):
            analysis["intent"] = "avg"
        elif any(word in query_lower for word in ['최대', '가장 큰', 'max']):
            analysis["intent"] = "max"
        elif any(word in query_lower for word in ['최소', '가장 작은', 'min']):
            analysis["intent"] = "min"
        
        # 시간 필터 분석
        time_patterns = [
            (r'최근 (\d+)일', 'recent_days'),
            (r'지난 (\d+)일', 'past_days'),
            (r'(\d{4})년', 'year'),
            (r'(\d{1,2})월', 'month'),
            (r'오늘', 'today'),
            (r'어제', 'yesterday'),
            (r'이번 주', 'this_week'),
            (r'지난 주', 'last_week'),
            (r'이번 달', 'this_month'),
            (r'지난 달', 'last_month')
        ]
        
        for pattern, time_type in time_patterns:
            match = re.search(pattern, user_query)
            if match:
                analysis["time_filters"].append({
                    "type": time_type,
                    "value": match.group(1) if match.groups() else None
                })
        
        # 정렬 분석
        if any(word in query_lower for word in ['top', '상위', '높은', '많은']):
            analysis["order_by"] = "desc"
        elif any(word in query_lower for word in ['bottom', '하위', '낮은', '적은']):
            analysis["order_by"] = "asc"
        
        # 제한 분석
        limit_match = re.search(r'(\d+)개', user_query)
        if limit_match:
            analysis["limit"] = int(limit_match.group(1))
        elif 'top' in query_lower:
            top_match = re.search(r'top\s*(\d+)', query_lower)
            if top_match:
                analysis["limit"] = int(top_match.group(1))
        
        return analysis
    
    def _generate_sql_query(self, user_query: str, schema_info: List[Dict], query_analysis: Dict) -> str:
        """스키마 정보를 기반으로 SQL 쿼리 생성"""
        try:
            # 첫 번째 테이블을 메인 테이블로 사용
            main_table = schema_info[0]
            table_name = main_table.get("table_name", "")
            columns = main_table.get("columns", [])
            
            if not table_name or not columns:
                return ""
            
            # SELECT 절 생성
            select_clause = self._build_select_clause(columns, query_analysis)
            
            # FROM 절 생성
            from_clause = f"FROM `{table_name}`"
            
            # WHERE 절 생성
            where_clause = self._build_where_clause(columns, query_analysis, user_query)
            
            # GROUP BY 절 생성 (집계 함수가 있는 경우)
            group_by_clause = self._build_group_by_clause(columns, query_analysis)
            
            # ORDER BY 절 생성
            order_by_clause = self._build_order_by_clause(columns, query_analysis)
            
            # LIMIT 절 생성
            limit_clause = self._build_limit_clause(query_analysis)
            
            # 최종 SQL 조합
            sql_parts = [f"SELECT {select_clause}", from_clause]
            
            if where_clause:
                sql_parts.append(f"WHERE {where_clause}")
            
            if group_by_clause:
                sql_parts.append(f"GROUP BY {group_by_clause}")
            
            if order_by_clause:
                sql_parts.append(f"ORDER BY {order_by_clause}")
            
            if limit_clause:
                sql_parts.append(f"LIMIT {limit_clause}")
            
            sql_query = "\n".join(sql_parts)
            
            return sql_query
            
        except Exception as e:
            print(f"❌ SQL 생성 중 오류: {str(e)}")
            return ""
    
    def _build_select_clause(self, columns: List[Dict], query_analysis: Dict) -> str:
        """SELECT 절 생성"""
        intent = query_analysis.get("intent", "select")
        
        # 기본적으로 처음 몇 개 컬럼 선택
        if intent == "select":
            # 주요 컬럼들을 선택 (최대 5개)
            main_columns = []
            for col in columns[:5]:
                col_name = col.get("name", "")
                if col_name:
                    main_columns.append(col_name)
            
            return ", ".join(main_columns) if main_columns else "*"
        
        elif intent == "count":
            return "COUNT(*) as total_count"
        
        elif intent in ["sum", "avg", "max", "min"]:
            # 숫자형 컬럼 찾기
            numeric_columns = [col for col in columns 
                             if col.get("type", "").upper() in ["INTEGER", "FLOAT", "NUMERIC", "DECIMAL"]]
            
            if numeric_columns:
                col_name = numeric_columns[0].get("name", "")
                return f"{intent.upper()}({col_name}) as {intent}_value"
            else:
                return "COUNT(*) as total_count"
        
        return "*"
    
    def _build_where_clause(self, columns: List[Dict], query_analysis: Dict, user_query: str) -> str:
        """WHERE 절 생성"""
        conditions = []
        
        # 시간 필터 처리
        date_columns = [col for col in columns 
                       if any(keyword in col.get("name", "").lower() 
                             for keyword in ["date", "time", "created", "updated", "timestamp"])]
        
        if date_columns and query_analysis.get("time_filters"):
            date_col = date_columns[0].get("name", "")
            time_filter = query_analysis["time_filters"][0]
            
            if time_filter["type"] == "recent_days":
                days = time_filter.get("value", "7")
                conditions.append(f"{date_col} >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)")
            elif time_filter["type"] == "past_days":
                days = time_filter.get("value", "7")
                conditions.append(f"{date_col} >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)")
            elif time_filter["type"] == "today":
                conditions.append(f"DATE({date_col}) = CURRENT_DATE()")
            elif time_filter["type"] == "this_month":
                conditions.append(f"EXTRACT(MONTH FROM {date_col}) = EXTRACT(MONTH FROM CURRENT_DATE())")
                conditions.append(f"EXTRACT(YEAR FROM {date_col}) = EXTRACT(YEAR FROM CURRENT_DATE())")
        
        return " AND ".join(conditions) if conditions else ""
    
    def _build_group_by_clause(self, columns: List[Dict], query_analysis: Dict) -> str:
        """GROUP BY 절 생성"""
        intent = query_analysis.get("intent", "select")
        
        # 집계 함수를 사용하는 경우에만 GROUP BY 필요
        if intent in ["count", "sum", "avg", "max", "min"]:
            # 카테고리형 컬럼 찾기
            category_columns = []
            for col in columns:
                col_type = col.get("type", "").upper()
                col_name = col.get("name", "").lower()
                
                if (col_type == "STRING" or 
                    any(keyword in col_name for keyword in ["category", "type", "status", "name", "id"])):
                    category_columns.append(col.get("name", ""))
            
            # 첫 번째 카테고리 컬럼 사용
            if category_columns:
                return category_columns[0]
        
        return ""
    
    def _build_order_by_clause(self, columns: List[Dict], query_analysis: Dict) -> str:
        """ORDER BY 절 생성"""
        order_direction = query_analysis.get("order_by")
        
        if not order_direction:
            return ""
        
        intent = query_analysis.get("intent", "select")
        
        # 집계 함수가 있는 경우 집계 결과로 정렬
        if intent == "count":
            return f"total_count {order_direction.upper()}"
        elif intent in ["sum", "avg", "max", "min"]:
            return f"{intent}_value {order_direction.upper()}"
        
        # 일반적인 경우 첫 번째 컬럼으로 정렬
        if columns:
            first_col = columns[0].get("name", "")
            return f"{first_col} {order_direction.upper()}"
        
        return ""
    
    def _build_limit_clause(self, query_analysis: Dict) -> str:
        """LIMIT 절 생성"""
        limit_value = query_analysis.get("limit")
        if limit_value:
            return str(limit_value)
        
        # 기본 제한값 (너무 많은 결과 방지)
        return "100"
    
    def _validate_sql(self, sql_query: str) -> Dict[str, Any]:
        """SQL 쿼리 기본 검증"""
        validation = {
            "valid": True,
            "warning": None
        }
        
        try:
            # 기본 구문 체크
            if not sql_query.strip():
                validation["valid"] = False
                validation["warning"] = "빈 쿼리입니다."
                return validation
            
            # SELECT가 있는지 확인
            if not sql_query.upper().strip().startswith("SELECT"):
                validation["valid"] = False
                validation["warning"] = "SELECT 문이 아닙니다."
                return validation
            
            # 위험한 키워드 체크
            dangerous_keywords = ["DROP", "DELETE", "UPDATE", "INSERT", "TRUNCATE", "ALTER"]
            for keyword in dangerous_keywords:
                if keyword in sql_query.upper():
                    validation["valid"] = False
                    validation["warning"] = f"위험한 키워드가 포함되어 있습니다: {keyword}"
                    return validation
            
            # 기본적인 구문 매칭 체크
            select_count = sql_query.upper().count("SELECT")
            from_count = sql_query.upper().count("FROM")
            
            if from_count == 0:
                validation["warning"] = "FROM 절이 없습니다."
            elif select_count != from_count:
                validation["warning"] = "SELECT와 FROM의 개수가 일치하지 않습니다."
            
            return validation
            
        except Exception as e:
            validation["valid"] = False
            validation["warning"] = f"SQL 검증 중 오류: {str(e)}"
            return validation


# 전역 SQLGenerator 인스턴스
sql_generator_agent = SQLGeneratorAgent()