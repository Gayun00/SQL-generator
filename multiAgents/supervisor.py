from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from pydantic.v1 import BaseModel, Field
from langchain_openai import ChatOpenAI

# LLM 정의
llm = ChatOpenAI(model="gpt-4o")

class Supervisor(BaseModel):
    """다중 에이전트의 작업을 조율하는 Supervisor."""
    next_agent: str = Field(
        description="다음으로 작업을 처리할 에이전트를 선택합니다.",
        enum=["SchemaAnalyzer", "SQLGenerator", "FINISH"]
    )

supervisor_prompt = ChatPromptTemplate.from_messages([
    ("system",
     "You are a supervisor. Your role is to manage the workflow between agents. "
     "Given the user's request, decide which agent should handle the task next. "
     "Available agents are: SchemaAnalyzer, SQLGenerator. "
     "Select 'FINISH' when the task is complete."),
    MessagesPlaceholder(variable_name="messages")
])

supervisor_chain = (
    supervisor_prompt
    | llm.with_structured_output(Supervisor)
)

def supervisor_node(state):
    """Supervisor의 결정을 바탕으로 다음 노드를 선택합니다."""
    print("--- SUPERVISOR ---")
    result = supervisor_chain.invoke(state)
    print(f"Supervisor choice: {result.next_agent}")
    return {"next": result.next_agent}
