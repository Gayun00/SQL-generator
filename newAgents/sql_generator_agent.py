"""
SQL Generator Agent - 자연어 쿼리를 SQL로 변환
"""

from typing import Dict, Any, List, Optional
import re
import json


class SQLGeneratorAgent:
    """자연어 쿼리를 기반으로 SQL을 생성하는 에이전트"""
    
    def __init__(self):
        """SQLGenerator Agent 초기화"""
        print("⚡ SQLGenerator Agent 초기화")
    
    async def generate_sql(self, user_query: str, schema_info: List[Dict]) -> Dict[str, Any]:
        """
        사용자 쿼리와 스키마 정보를 기반으로 SQL 생성
        
        Args:
            user_query: 사용자 자연어 쿼리
            schema_info: 관련 스키마 정보
            
        Returns:
            SQL 생성 결과
        """
        try:
            print(f"⚡ SQL 생성 시작: {user_query}")
            
            # 스키마 정보 검증
            if not schema_info:
                return {
                    "success": False,
                    "error": "관련 스키마 정보가 없습니다. 다른 키워드로 시도해보세요.",
                    "sql_query": ""
                }
            
            # 쿼리 분석
            query_analysis = self._analyze_query(user_query)
            
            # 스키마 기반 SQL 생성
            sql_query = self._generate_sql_query(user_query, schema_info, query_analysis)
            
            if not sql_query:
                return {
                    "success": False,
                    "error": "SQL 쿼리 생성에 실패했습니다.",
                    "sql_query": ""
                }
            
            # SQL 검증
            validation_result = self._validate_sql(sql_query)
            if not validation_result["valid"]:
                print(f"⚠️ SQL 검증 경고: {validation_result['warning']}")
            
            print(f"✅ SQL 생성 완료")
            
            return {
                "success": True,
                "sql_query": sql_query,
                "query_analysis": query_analysis,
                "schema_info": schema_info,
                "validation": validation_result,
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