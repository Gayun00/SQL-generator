from langchain_core.tools import tool

@tool
def get_schema_info(db_name: str) -> str:
    """데이터베이스 스키마 정보를 얻기 위한 도구. 이 도구를 사용하여 특정 데이터베이스의 테이블 정보를 얻을 수 있습니다."""
    print(f"Getting schema for: {db_name}")
    return f"Table info for {db_name}: users(id, name, email), products(id, name, price)"
