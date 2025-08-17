import functools
from typing import Dict, Any
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage
from langgraph.graph import StateGraph, END

from multiAgents.tools.sql import generate_sql
from multiAgents.state import AgentState
from multiAgents.config import DEBUG

# LLM 정의
llm = ChatOpenAI(model="gpt-4o")

def sql_generation_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    SQL 생성 노드 - 사용자 요청과 스키마 정보를 기반으로 SQL을 생성
    """
    if DEBUG:
        print("\n🔧 SQL GENERATION NODE")
        print("="*30)
    
    # 사용자 요청과 스키마 정보 추출
    messages = state.get("messages", [])
    user_request = ""
    schema_info = ""
    
    # 메시지에서 사용자 요청과 스키마 정보 추출
    for msg in reversed(messages):
        if isinstance(msg, HumanMessage):
            user_request = msg.content
            break
    
    # 스키마 정보는 이전 메시지들에서 찾기
    for msg in messages:
        if isinstance(msg, AIMessage) and "table" in msg.content.lower():
            schema_info = msg.content
            break
    
    if not schema_info:
        schema_info = "No schema information available"
    
    try:
        # SQL 생성 툴 사용
        sql_result = generate_sql.invoke({
            "question": user_request,
            "schema_info": schema_info
        })
        
        # 결과 메시지 생성
        result_message = AIMessage(
            content=f"Generated SQL query:\n```sql\n{sql_result}\n```\n\nThis query addresses your request: {user_request}"
        )
        
        if DEBUG:
            print(f"✅ SQL generated: {sql_result}")
        
        return {
            **state,
            "messages": messages + [result_message]
        }
        
    except Exception as e:
        error_message = AIMessage(content=f"SQL generation failed: {str(e)}")
        return {
            **state,
            "messages": messages + [error_message]
        }

# SQL Generator 그래프 생성
def create_sql_generator_graph():
    """SQL Generator 워크플로우 그래프 생성"""
    
    graph = StateGraph(AgentState)
    
    # 노드 추가
    graph.add_node("GenerateSQL", sql_generation_node)
    
    # 워크플로우 정의
    graph.set_entry_point("GenerateSQL")
    graph.add_edge("GenerateSQL", END)
    
    return graph.compile()

# SQL Generator 그래프 컴파일
sql_generator_graph = create_sql_generator_graph()

def sql_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    SQL Generator 메인 노드
    """
    if DEBUG:
        print("\n" + "="*50)
        print("🔧 SQL GENERATOR STARTING")
        print("="*50)
    
    try:
        # 내부 그래프 실행
        result = sql_generator_graph.invoke(state)
        
        if DEBUG:
            print("="*50)
            print("🔧 SQL GENERATOR COMPLETED")
            print("="*50)
        
        return result
        
    except Exception as e:
        if DEBUG:
            print(f"❌ SQL Generator Error: {str(e)}")
        
        error_message = AIMessage(content=f"SQL generation failed: {str(e)}")
        return {
            **state,
            "messages": state.get("messages", []) + [error_message]
        }
