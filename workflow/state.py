from typing import TypedDict, Optional, List, Dict

class SQLGeneratorState(TypedDict):
    userInput: str
    isValid: bool
    reason: Optional[str]
    schemaInfo: Optional[Dict]
    sqlQuery: Optional[str]
    explanation: Optional[str]
    finalOutput: Optional[str]
    # Phase 1: SQL 실행 관련 필드
    queryResults: Optional[Dict]      # SQL 실행 결과
    executionStatus: Optional[str]    # 실행 상태 (success/failed)