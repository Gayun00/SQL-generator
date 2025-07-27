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
    # Phase 2: 불확실성 분석 관련 필드
    uncertaintyAnalysis: Optional[Dict]  # 불확실성 분석 결과
    hasUncertainty: Optional[bool]       # 불확실성 존재 여부
    explorationResults: Optional[Dict]   # 탐색 쿼리 실행 결과