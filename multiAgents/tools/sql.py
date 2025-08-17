from langchain_core.tools import tool

@tool
def generate_sql(question: str, schema_info: str) -> str:
    """사용자 질문과 스키마 정보를 바탕으로 SQL 쿼리를 생성하기 위한 도구."""
    print(f"Generating SQL for: '{question}' with schema: '{schema_info}'")
    return f"SELECT * FROM users WHERE name = 'test';"
