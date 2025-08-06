from typing import Dict, Any, List, Optional
import random

# Mock LLM Client (실제 ChatOpenAI 역할을 시뮬레이션)
class MockChatOpenAI:
    def __init__(self, model: str):
        self.model = model

    async def generate_sql(self, user_query: str, schema_info: List[Dict]) -> Optional[str]:
        print(f"[LLM Mock] Generating SQL for query: '{user_query}' with schema: {schema_info}")
        # 시뮬레이션: 특정 쿼리에 대해 성공/실패 시뮬레이션
        if "사용자 목록" in user_query and any(s.get("table_name") == "users" for s in schema_info):
            return "SELECT * FROM users;"
        elif "매출" in user_query and any(s.get("table_name") == "orders" for s in schema_info):
            return "SELECT SUM(amount) FROM orders;"
        elif "실패" in user_query:
            return None # SQL 생성 실패 시뮬레이션
        else:
            return f"SELECT * FROM some_table WHERE query = '{user_query}';" # 기본 성공

# SQL Generator Agent
class SQLGenerator:
    def __init__(self):
        self.llm = MockChatOpenAI(model="gpt-4") # 실제 ChatOpenAI 인스턴스 사용

    async def process(self, message: Dict[str, Any]) -> Dict[str, Any]:
        session_id = message["session_id"]
        user_query = message["content"]["user_query"]
        schema_info = message["content"]["schema_info"]

        print(f"[SQL Generator] Processing for session {session_id} with query: {user_query}")

        generated_sql = await self.llm.generate_sql(user_query, schema_info)

        if generated_sql:
            print(f"[SQL Generator] Successfully generated SQL: {generated_sql[:50]}...")
            return {
                "status": "success",
                "generated_sql": generated_sql,
                "next_agent": "sql_executor"
            }
        else:
            print("[SQL Generator] Failed to generate SQL.")
            return {
                "status": "error",
                "error_message": "SQL generation failed.",
                "next_agent": "error_handler"
            }