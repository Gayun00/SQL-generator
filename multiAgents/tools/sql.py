from pydantic.v1 import BaseModel, Field

class QueryInfo(BaseModel):
    """SQL 쿼리 생성을 위한 도구"""
    question: str = Field(description="SQL로 변환하고 싶은 사용자 질문")
    schema_info: str = Field(description="참고할 스키마 정보")

def generate_sql(question: str, schema_info: str) -> str:
    """사용자 질문과 스키마를 바탕으로 SQL 쿼리를 생성합니다 (임시)."""
    print(f"Generating SQL for: '{question}' with schema: '{schema_info}'")
    return f"SELECT * FROM users WHERE name = 'test';"
