from typing import Annotated, Any, Sequence, TypedDict

class AgentState(TypedDict):
    """에이전트 시스템 전체에서 공유될 상태"""
    messages: Sequence[Annotated[Any, "MessagesPlaceholder"]]
    next: str
