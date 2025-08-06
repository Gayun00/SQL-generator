
import random
from typing import Dict, Any, List, Optional

# (New) Data Explorer
async def data_explorer(state: Dict[str, Any]) -> Dict[str, Any]:
    """Simulates exploring data in the database to get column values."""
    print("----- 3a. Data Explorer -----")
    info_request = state.get("sql_generator_result", {}).get("info_request", {})
    print(f"Exploring data for: {info_request.get('query')}")
    return {
        "status": "success",
        "data_sample": ["John Doe", "Jane Smith"],
        "confidence": 0.9
    }

# 4. SQL Executor
async def sql_executor(state: Dict[str, Any]) -> Dict[str, Any]:
    """Simulates SQL execution."""
    print("----- 4. SQL Executor -----")
    sql = state.get("sql_generator_result", {}).get("generated_sql", "")
    
    # 항상 성공을 반환하도록 수정
    print("SQL execution successful (mocked).")
    return {
        "status": "success",
        "execution_result": [{"id": 1, "name": "John Doe", "signup_date": "2025-01-15"}]
    }
