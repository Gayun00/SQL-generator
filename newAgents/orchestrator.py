from typing import TypedDict, List, Dict, Any, Optional
import uuid
from langgraph.graph import StateGraph, END
from newAgents.sub_agents import user_communicator, schema_analyzer, sql_generator, sql_executor, data_explorer

# 1. State Definition
class OrchestratorState(TypedDict):
    original_query: str
    session_id: str
    is_complete: bool
    # Agent results store the direct output of each agent
    user_communicator_result: Optional[Dict[str, Any]]
    schema_analyzer_result: Optional[Dict[str, Any]]
    sql_generator_result: Optional[Dict[str, Any]]
    sql_executor_result: Optional[Dict[str, Any]]
    data_explorer_result: Optional[Dict[str, Any]]
    # Control flow and history
    error_message: Optional[str]
    workflow_history: List[str]
    final_result: Optional[Dict[str, Any]]

# 2. Orchestrator Class
class Orchestrator:
    def __init__(self):
        self.workflow = StateGraph(OrchestratorState)
        self._setup_workflow()

    def _setup_workflow(self):
        # Node for each agent
        self.workflow.add_node("user_communicator", self.run_user_communicator)
        self.workflow.add_node("schema_analyzer", self.run_schema_analyzer)
        self.workflow.add_node("data_explorer", self.run_data_explorer)
        self.workflow.add_node("sql_generator", self.run_sql_generator)
        self.workflow.add_node("sql_executor", self.run_sql_executor)
        self.workflow.add_node("error_handler", self.error_handler)

        # Entry point
        self.workflow.set_entry_point("user_communicator")

        # Edges and conditional routing
        self.workflow.add_conditional_edges("user_communicator", self.route_from_user_communicator)
        self.workflow.add_conditional_edges("schema_analyzer", self.route_from_schema_analyzer)
        self.workflow.add_conditional_edges("sql_generator", self.route_from_sql_generator)
        self.workflow.add_conditional_edges("sql_executor", self.route_from_sql_executor)
        
        self.workflow.add_edge("data_explorer", "sql_generator")
        self.workflow.add_edge("error_handler", END)

        self.app = self.workflow.compile()

    # --- Agent Node Functions ---
    def run_user_communicator(self, state: OrchestratorState) -> OrchestratorState:
        state["workflow_history"].append("Running User Communicator")
        result = user_communicator(state)
        state["user_communicator_result"] = result
        return state

    def run_schema_analyzer(self, state: OrchestratorState) -> OrchestratorState:
        state["workflow_history"].append("Running Schema Analyzer")
        result = schema_analyzer(state)
        state["schema_analyzer_result"] = result
        return state

    def run_data_explorer(self, state: OrchestratorState) -> OrchestratorState:
        state["workflow_history"].append("Running Data Explorer")
        result = data_explorer(state)
        state["data_explorer_result"] = result
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

    def error_handler(self, state: OrchestratorState) -> OrchestratorState:
        state["workflow_history"].append("Workflow Error")
        state["is_complete"] = True
        state["error_message"] = "The workflow hit an unrecoverable error."
        print(f"---" + state["error_message"] + "---")
        return state

    # --- Routing Functions ---
    def route_from_user_communicator(self, state: OrchestratorState) -> str:
        status = state["user_communicator_result"]["status"]
        return "schema_analyzer" if status == "success" else "user_communicator"

    def route_from_schema_analyzer(self, state: OrchestratorState) -> str:
        status = state["schema_analyzer_result"]["status"]
        if status == "success": return "sql_generator"
        if status == "insufficient_info": return "user_communicator"
        return "error_handler"

    def route_from_sql_generator(self, state: OrchestratorState) -> str:
        status = state["sql_generator_result"]["status"]
        if status == "success": return "sql_executor"
        if status == "need_more_info":
            return "data_explorer" if state["sql_generator_result"]["info_request"]["type"] == "db_info" else "user_communicator"
        return "error_handler"

    def route_from_sql_executor(self, state: OrchestratorState) -> str:
        status = state["sql_executor_result"]["status"]
        if status == "success":
            state["is_complete"] = True
            state["final_result"] = state["sql_executor_result"]
            return END
        return "sql_generator" # For revision

    # --- Runner ---
    def run(self, query: str):
        initial_state = {
            "original_query": query,
            "session_id": str(uuid.uuid4()),
            "is_complete": False,
            "user_communicator_result": None, "schema_analyzer_result": None,
            "sql_generator_result": None, "sql_executor_result": None,
            "data_explorer_result": None, "error_message": None,
            "workflow_history": [], "final_result": None,
        }
        final_state = self.app.invoke(initial_state, {"recursion_limit": 15})
        return final_state

if __name__ == '__main__':
    orchestrator = Orchestrator()
    print("--- Running Scenario 1: Self-Correction ---")
    result1 = orchestrator.run("Show me John Doe's record including signup date")
    print("\nFinal Result (Scenario 1):", result1.get('final_result'))
    
    print("\n--- Running Scenario 2: Needs Clarification & Loop Prevention ---")
    result2 = orchestrator.run("show data")
    print("\nFinal Result (Scenario 2):", result2.get('final_result') or result2.get('user_communicator_result'))
