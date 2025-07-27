from typing import TypedDict, Optional, List, Dict

class SQLGeneratorState(TypedDict):
    userInput: str
    isValid: bool
    reason: Optional[str]
    schemaInfo: Optional[Dict]
    sqlQuery: Optional[str]
    explanation: Optional[str]
    finalOutput: Optional[str]