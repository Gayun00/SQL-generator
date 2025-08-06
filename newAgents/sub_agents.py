import random
from typing import Dict, Any, List, Optional

# 1. User Communicator
def user_communicator(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simulates user interaction. 
    - If the query is too simple, it asks for clarification.
    - Otherwise, it processes the query.
    """
    print("----- 1. User Communicator -----")
    original_query = state.get("original_query", "")
    
    # Schema Analyzer가 추가 정보를 요청한 경우
    schema_analyzer_result = state.get("schema_analyzer_result")
    if schema_analyzer_result and schema_analyzer_result.get("status") == "insufficient_info":
        print("Clarifying based on Schema Analyzer's request.")
        return {
            "status": "success",
            "processed_query": f"{original_query} about 'customers' table",
            "clarification_question": None,
            "confidence": 0.95
        }

    # 초기 입력이 너무 간단하고, 재질문 기록이 없는 경우
    user_comm_result = state.get("user_communicator_result")
    if len(original_query.split()) < 3 and not (user_comm_result and user_comm_result.get("clarification_question")):
        print("Query is too short, asking for clarification.")
        return {
            "status": "needs_clarification",
            "processed_query": original_query,
            "clarification_question": "Could you please provide more details about your request?",
            "confidence": 0.4
        }
    
    print("Query is sufficient.")
    return {
        "status": "success",
        "processed_query": original_query,
        "clarification_question": None,
        "confidence": 0.9
    }

# 2. Schema Analyzer
def schema_analyzer(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simulates schema analysis.
    - If the query lacks specific keywords, it requests more info.
    """
    print("----- 2. Schema Analyzer -----")
    query = state.get("user_communicator_result", {}).get("processed_query", "")
    
    # 'customers' 또는 'orders' 같은 키워드가 없으면 정보 부족 시뮬레이션
    if "customers" not in query and "orders" not in query:
        print("Query lacks keywords, requesting more info.")
        return {
            "status": "insufficient_info",
            "schema_info": None,
            "confidence": 0.5,
            "additional_info_needed": "Which table are you interested in (e.g., customers, orders)?"
        }
        
    print("Found relevant schema.")
    return {
        "status": "success",
        "schema_info": {"customers": ["id", "name", "signup_date"], "orders": ["id", "customer_id", "amount", "date"]},
        "confidence": 0.95,
        "additional_info_needed": None
    }

# (New) Data Explorer
def data_explorer(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simulates exploring data in the database to get column values.
    """
    print("----- 3a. Data Explorer -----")
    info_request = state.get("sql_generator_result", {}).get("info_request", {})
    
    print(f"Exploring data for: {info_request.get('query')}")
    
    return {
        "status": "success",
        "data_sample": ["John Doe", "Jane Smith"],
        "confidence": 0.9
    }

# 3. SQL Generator
def sql_generator(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simulates SQL generation.
    - May request more info from user or data explorer.
    - May receive revised instructions from the executor.
    """
    print("----- 3. SQL Generator -----")
    
    # SQLExecutor가 수정을 요청한 경우
    sql_executor_result = state.get("sql_executor_result")
    if sql_executor_result and sql_executor_result.get("status") == "error":
        print("Revising SQL based on executor feedback.")
        return {
            "status": "success",
            "generated_sql": "SELECT id, name, signup_date FROM customers WHERE name = 'John Doe'", # 수정된 쿼리
            "confidence": 0.98,
            "info_request": None
        }

    # Data Explorer로부터 데이터를 받은 후
    if state.get("data_explorer_result"):
        print("Generating SQL with data from Data Explorer.")
        return {
            "status": "success",
            "generated_sql": "SELECT * FROM customers WHERE name IN ('John Doe', 'Jane Smith')",
            "confidence": 0.95,
            "info_request": None
        }

    # 특정 이름(e.g., 'latest customer')을 모를 경우 Data Explorer에게 요청
    if "latest customer" in state.get("user_communicator_result", {}).get("processed_query", ""):
        print("Need to find the latest customer's name from the DB.")
        return {
            "status": "need_more_info",
            "generated_sql": None,
            "confidence": 0.7,
            "info_request": {
                "type": "db_info",
                "query": "Find the name of the most recent customer."
            }
        }
        
    print("Generating initial SQL.")
    # 초기 SQL 생성 (의도적으로 오류가 있는 쿼리 생성)
    return {
        "status": "success",
        "generated_sql": "SELECT * FROM customers WHERE name = 'John Doe'", # 오류: signup_date 없음
        "confidence": 0.8,
        "info_request": None
    }

# 4. SQL Executor
def sql_executor(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simulates SQL execution.
    - If the SQL is missing a column, it returns an error to trigger self-correction.
    """
    print("----- 4. SQL Executor -----")
    sql = state.get("sql_generator_result", {}).get("generated_sql", "")
    
    # 'signup_date'가 없으면 에러 시뮬레이션
    if "signup_date" not in sql:
        print("SQL execution failed: 'signup_date' column is missing.")
        return {
            "status": "error",
            "execution_result": None,
            "error_analysis": "The query is likely missing the 'signup_date' column which is required for the final output.",
            "retry_suggestion": {
                "instruction": "Regenerate the SQL to include the 'id', 'name', and 'signup_date' columns from the 'customers' table."
            }
        }
    
    print("SQL execution successful.")
    return {
        "status": "success",
        "execution_result": [{"id": 1, "name": "John Doe", "signup_date": "2025-01-15"}],
        "error_analysis": None,
        "retry_suggestion": None
    }