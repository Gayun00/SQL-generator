from typing import TypedDict, Optional, List

class ScheduleState(TypedDict):
    userInput: str
    isValid: bool
    reason: Optional[str]
    plan: Optional[List[str]]
    finalOutput: Optional[str]