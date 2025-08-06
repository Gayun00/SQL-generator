
from typing import Dict, Any, Optional

# Mock LLM Client (실제 ChatOpenAI 역할을 시뮬레이션)
class MockLLMClient:
    async def check_ambiguity(self, query: str) -> bool:
        ambiguous_keywords = ['매출', '데이터', '결과', '보여줘']
        specific_keywords = ['고객', '주문', '상품', '최근', '지난달']
        if not any(key in query for key in specific_keywords) and any(key in query for key in ambiguous_keywords):
            print("[LLM SIM] Ambiguity detected.")
            return True
        print("[LLM SIM] Query is specific enough.")
        return False

    async def translate_request_to_question(self, request: Dict[str, Any]) -> str:
        print(f"[LLM SIM] Translating request: {request}")
        if request and request.get("type") == "table_disambiguation":
            tables = ", ".join(request.get("options", []))
            return f"어떤 테이블의 데이터를 찾으시나요? (옵션: {tables})"
        return "추가 정보가 필요합니다. 좀 더 자세히 설명해주시겠어요?"

# 자율성이 강화된 UserCommunicator
class UserCommunicator:
    def __init__(self):
        self.session_contexts = {}
        self.llm_client = MockLLMClient()

    def _get_context(self, session_id: str) -> Dict[str, Any]:
        """세션 ID에 해당하는 컨텍스트를 가져오거나 생성합니다."""
        if session_id not in self.session_contexts:
            self.session_contexts[session_id] = {
                "pending_question": None,
                "requesting_agent": None,
                "conversation_history": []
            }
        return self.session_contexts[session_id]

    async def process(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Orchestrator와의 단일 소통 창구"""
        session_id = message["session_id"]
        context = self._get_context(session_id)
        context["conversation_history"].append(message)

        # 1. 사용자 응답 처리 (가장 높은 우선순위)
        if message.get("user_response"):
            response = message["user_response"]
            next_agent = context.pop("requesting_agent", "schema_analyzer")
            context["pending_question"] = None
            return {
                "status": "success",
                "needs_user_interaction": False,
                "processed_result": {"user_input": response},
                "next_agent": next_agent
            }

        # 2. 다른 에이전트의 요청 처리
        elif message.get("from_agent"):
            agent = message["from_agent"]
            question = await self.llm_client.translate_request_to_question(message.get("content"))
            context["pending_question"] = question
            context["requesting_agent"] = agent
            return {
                "status": "needs_user_input",
                "needs_user_interaction": True,
                "question": question,
                "next_agent": "user_communicator"  # 응답을 받기 위해 자신을 다시 호출
            }

        # 3. 최초 사용자 입력 처리
        else:
            query = message["content"]["query"]
            if await self.llm_client.check_ambiguity(query):
                question = "어떤 데이터를 찾으시는지 좀 더 자세히 알려주시겠어요?"
                context["pending_question"] = question
                context["requesting_agent"] = "schema_analyzer"
                return {
                    "status": "needs_user_input",
                    "needs_user_interaction": True,
                    "question": question,
                    "next_agent": "user_communicator" # 응답을 받기 위해 자신을 다시 호출
                }
            return {
                "status": "success",
                "needs_user_interaction": False,
                "processed_result": {"user_input": query},
                "next_agent": "schema_analyzer"
            }
