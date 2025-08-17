from pydantic.v1 import BaseModel, Field

class SchemaInfo(BaseModel):
    """데이터베이스 스키마 정보를 얻기 위한 도구"""
    db_name: str = Field(description="정보를 얻고 싶은 데이터베이스 이름")

def get_schema_info(db_name: str) -> str:
    """데이터베이스 스키마 정보를 반환합니다 (임시)."""
    print(f"Getting schema for: {db_name}")
    return f"Table info for {db_name}: users(id, name, email), products(id, name, price)"
