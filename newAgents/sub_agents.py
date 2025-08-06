
import random
from typing import Dict, Any, List, Optional

# 2. Schema Analyzer
def schema_analyzer(state: Dict[str, Any]) -> Dict[str, Any]:
    """Dummy Schema Analyzer Agent."""
    print("----- 2. Schema Analyzer -----")
    query = state.get("query", "")
    
    # 'customers'/'고객' 또는 'orders'/'주문' 같은 키워드가 없으면 정보 부족 시뮬레이션
    if all(kw not in query for kw in ["customers", "orders", "고객", "주문"]):
        print("Query lacks keywords, requesting more info.")
        return {
            "status": "insufficient_info",
            "confidence": 0.5,
            "info_request": {
                "type": "table_disambiguation",
                "options": ["customers", "orders"]
            }
        }
        
    print("Found relevant schema.")
    return {
        "status": "success",
        "schema_info": {"customers": ["id", "name", "signup_date"], "orders": ["id", "customer_id", "amount", "date"]},
        "confidence": 0.95
    }

# (New) Data Explorer
def data_explorer(state: Dict[str, Any]) -> Dict[str, Any]:
    """Simulates exploring data in the database to get column values."""
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
    """Simulates SQL generation."""
    print("----- 3. SQL Generator -----")
    
    # SQLExecutor가 수정을 요청한 경우
    if state.get("sql_executor_result", {}).get("status") == "error":
        print("Revising SQL based on executor feedback.")
        return {
            "status": "success",
            "generated_sql": "SELECT id, name, signup_date FROM customers WHERE name = 'John Doe'",
            "confidence": 0.98
        }

    # Data Explorer로부터 데이터를 받은 후
    if state.get("data_explorer_result"):
        print("Generating SQL with data from Data Explorer.")
        return {
            "status": "success",
            "generated_sql": "SELECT * FROM customers WHERE name IN ('John Doe', 'Jane Smith')",
            "confidence": 0.95
        }

    # 특정 이름(e.g., 'latest customer')을 모를 경우 Data Explorer에게 요청
    if "latest customer" in state.get("user_input", ""):
        print("Need to find the latest customer's name from the DB.")
        return {
            "status": "need_more_info",
            "info_request": {
                "type": "db_info",
                "query": "Find the name of the most recent customer."
            }
        }
        
    print("Generating initial SQL.")
    return {
        "status": "success",
        "generated_sql": "SELECT * FROM customers WHERE name = 'John Doe'",
        "confidence": 0.8
    }

# 4. SQL Executor
def sql_executor(state: Dict[str, Any]) -> Dict[str, Any]:
    """Simulates SQL execution."""
    print("----- 4. SQL Executor -----")
    sql = state.get("sql_generator_result", {}).get("generated_sql", "")
    
    if "signup_date" not in sql:
        print("SQL execution failed: 'signup_date' column is missing.")
        return {
            "status": "error",
            "retry_suggestion": {
                "instruction": "Regenerate the SQL to include the 'id', 'name', and 'signup_date' columns."
            }
        }
    
    print("SQL execution successful.")
    return {
        "status": "success",
        "execution_result": [{"id": 1, "name": "John Doe", "signup_date": "2025-01-15"}]
    }
