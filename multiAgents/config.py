"""
Multi-Agent 시스템 설정 파일
"""

# 에이전트 정의
AGENTS = {
    "SchemaAnalyzer": {
        "description": "Retrieves and analyzes database schema information",
        "capabilities": [
            "Get table structures",
            "Identify column types", 
            "Extract relationships"
        ]
    },
    "SQLGenerator": {
        "description": "Generates SQL queries based on requirements",
        "capabilities": [
            "Create SELECT queries",
            "Build complex JOINs",
            "Generate aggregate queries"
        ]
    }
}

# LLM 모델 설정
LLM_MODEL = "gpt-4o"

# 재귀 제한 설정
DEFAULT_RECURSION_LIMIT = 5

# 디버그 모드
DEBUG = True

# Human-in-the-Loop 설정
HUMAN_IN_THE_LOOP = True

# 테스트 모드 (자동으로 choice 1 선택)
TEST_MODE = False