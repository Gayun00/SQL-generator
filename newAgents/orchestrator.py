from typing import TypedDict, List, Dict, Any, Optional
import uuid
from langgraph.graph import StateGraph, END
from newAgents.user_communicator_agent import UserCommunicator
from newAgents.schema_analyzer_agent import SchemaAnalyzer
from newAgents.sub_agents import sql_generator, sql_executor, data_explorer

# 1. State Definition - retry_counts 추가
class OrchestratorState(TypedDict):
    session_id: str
    is_complete: bool
    user_input: Optional[str]
    user_response: Optional[str]
    agent_request_content: Optional[Dict[str, Any]]
    schema_analyzer_result: Optional[Dict[str, Any]]
    sql_generator_result: Optional[Dict[str, Any]]
    sql_executor_result: Optional[Dict[str, Any]]
    data_explorer_result: Optional[Dict[str, Any]]
    next_agent_to_call: str
    requesting_agent: Optional[str]
    error_message: Optional[str]
    workflow_history: List[str]
    final_result: Optional[Dict[str, Any]]
    pending_question: Optional[str]
    retry_counts: Dict[str, int] # 재시도 횟수 추가

# 2. Orchestrator Class
class Orchestrator:
    def __init__(self):
        self.user_communicator = UserCommunicator()
        self.schema_analyzer = SchemaAnalyzer()
        self.workflow = StateGraph(OrchestratorState)
        self._setup_workflow()

    def _setup_workflow(self):
        self.workflow.add_node("user_communicator", self.run_user_communicator)
        self.workflow.add_node("schema_analyzer", self.run_schema_analyzer)
        self.workflow.add_node("data_explorer", self.run_data_explorer)
        self.workflow.add_node("sql_generator", self.run_sql_generator)
        self.workflow.add_node("sql_executor", self.run_sql_executor)
        self.workflow.add_node("error_handler", self.error_handler)
        
        self.workflow.set_entry_point("user_communicator")

        self.workflow.add_conditional_edges("user_communicator", self.route_from_user_communicator)
        self.workflow.add_conditional_edges("schema_analyzer", self.route_from_schema_analyzer)
        self.workflow.add_conditional_edges("sql_generator", self.route_from_sql_generator)
        self.workflow.add_conditional_edges("sql_executor", self.route_from_sql_executor)
        
        self.workflow.add_edge("data_explorer", "sql_generator")
        self.workflow.add_edge("error_handler", END)

        self.app = self.workflow.compile()

    # --- Agent Node Functions ---
    def run_user_communicator(self, state: OrchestratorState) -> OrchestratorState:
        state["workflow_history"].append("Contacting User Communicator")
        
        message = {"session_id": state["session_id"]}
        if state.get("user_response"):
            message["user_response"] = state["user_response"]
        elif state.get("requesting_agent"):
            message["from_agent"] = state["requesting_agent"]
            message["content"] = state.get("agent_request_content")
        else:
            message["content"] = {"query": state.get("user_input")}

        result = self.user_communicator.process(message)

        state["next_agent_to_call"] = result.get("next_agent")
        if result.get("processed_result"):
            state["user_input"] = result["processed_result"].get("user_input")
        state["pending_question"] = result.get("question") if result.get("needs_user_interaction") else None
        
        state["user_response"] = None
        state["requesting_agent"] = None
        state["agent_request_content"] = None
        return state

    def run_schema_analyzer(self, state: OrchestratorState) -> OrchestratorState:
        state["workflow_history"].append("Running Schema Analyzer")
        result = self.schema_analyzer.process({
            "session_id": state["session_id"],
            "content": {"query": state["user_input"]}
        })
        state["schema_analyzer_result"] = result
        return state

    def run_sql_generator(self, state: OrchestratorState) -> OrchestratorState:
        state["workflow_history"].append("Running SQL Generator")
        result = sql_generator(state)
        state["sql_generator_result"] = result
        return state

    def run_sql_executor(self, state: OrchestratorState) -> OrchestratorState:
        state["workflow_history"].append("Running SQL Executor")
        result = sql_executor(state)
        state["sql_executor_result"] = result
        return state

    def run_data_explorer(self, state: OrchestratorState) -> OrchestratorState:
        state["workflow_history"].append("Running Data Explorer")
        result = data_explorer(state)
        state["data_explorer_result"] = result
        return state

    def error_handler(self, state: OrchestratorState) -> OrchestratorState:
        state["workflow_history"].append("Workflow Error")
        state["is_complete"] = True
        return state

    # --- Routing Functions ---
    def route_from_user_communicator(self, state: OrchestratorState) -> str:
        if state.get("pending_question"):
            return END
        return state.get("next_agent_to_call", "error_handler")

    def route_from_schema_analyzer(self, state: OrchestratorState) -> str:
        status = state["schema_analyzer_result"]["status"]
        if status == "success":
            return "sql_generator"
        if status == "needs_user_clarification":
            state["requesting_agent"] = "schema_analyzer"
            state["agent_request_content"] = {"question": state["schema_analyzer_result"].get("question")}
            return "user_communicator"
        return "error_handler"

    def route_from_sql_generator(self, state: OrchestratorState) -> str:
        status = state["sql_generator_result"]["status"]
        if status == "success":
            return "sql_executor"
        if status == "need_more_info":
            state["requesting_agent"] = "sql_generator"
            state["agent_request_content"] = state["sql_generator_result"].get("info_request")
            return "data_explorer" if state["sql_generator_result"]["info_request"]["type"] == "db_info" else "user_communicator"
        return "error_handler"

    def route_from_sql_executor(self, state: OrchestratorState) -> str:
        status = state["sql_executor_result"]["status"]
        if status == "success":
            state["is_complete"] = True
            state["final_result"] = state["sql_executor_result"]
            return END
        state["requesting_agent"] = "sql_executor"
        state["agent_request_content"] = state["sql_executor_result"].get("retry_suggestion")
        return "sql_generator"

    # --- Test Runner (루프 횟수 제한 추가) ---
    def run_test_scenario(self, initial_query: str, user_responses: List[str] = [], max_loop_iterations: int = 3):
        state = {
            "session_id": str(uuid.uuid4()),
            "user_input": initial_query,
            "workflow_history": [],
            "retry_counts": {}
        }
        response_index = 0
        loop_count = 0 # 루프 카운터 추가
        
        print(f"\n--- Starting Scenario with initial query: {initial_query} ---")

        while not state.get("is_complete") and loop_count < max_loop_iterations: # 루프 횟수 제한 조건 추가
            loop_count += 1
            print(f"\n--- Loop Iteration: {loop_count} ---") # 디버깅을 위한 출력

            # Run the graph until it stops (END or pending_question)
            new_state = self.app.invoke(state, {"recursion_limit": 25})
            
            # 새로운 state에서 final_result가 설정되었는지 확인
            if new_state.get("sql_executor_result", {}).get("status") == "success" and not state.get("final_result"):
                new_state["final_result"] = new_state["sql_executor_result"]
                new_state["is_complete"] = True
            
            state = new_state

            # If the graph stopped to ask a question, handle it.
            if state.get("pending_question"):
                if response_index < len(user_responses):
                    print(f"\n[질문] {state['pending_question']}")
                    user_response = user_responses[response_index]
                    print(f"> {user_response}")
                    
                    state["user_response"] = user_response
                    state["pending_question"] = None
                    response_index += 1
                else:
                    print("Error: Not enough user responses for the scenario.")
                    state["is_complete"] = True
                    state["error_message"] = "Not enough user responses provided."
                    break
            
            # Check for completion or explicit END
            if state.get("is_complete") or state.get("next_agent_to_call") == END:
                break

        if loop_count >= max_loop_iterations and not state.get("is_complete"):
            print(f"\n--- Max loop iterations ({max_loop_iterations}) reached. Terminating workflow. ---")
            state["is_complete"] = True
            state["error_message"] = "Max loop iterations reached."

        print("\n--- Workflow Finished ---")
        print("Final Result:", state.get("final_result"))
        print("History:", state.get("workflow_history"))

if __name__ == '__main__':
    orchestrator = Orchestrator()
    print("--- Running Scenario 1: Ambiguous Query ---")
    orchestrator.run_test_scenario("매출 보여줘", user_responses=["지난달 주문 데이터 보여줘"])

    print("\n--- Running Scenario 2: Agent Request for Clarification ---")
    orchestrator.run_test_scenario("최근 가입한 사람 정보", user_responses=["customers"])
