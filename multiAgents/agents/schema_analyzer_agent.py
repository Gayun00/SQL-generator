import functools
from typing import Dict, Any
from langchain_core.messages import HumanMessage, AIMessage
from langgraph.graph import StateGraph, END

from multiAgents.tools.schema_analyzer.clarifier import clarifier_tool
from multiAgents.tools.schema_analyzer.retrieve_schema import retrieve_schema_tool, format_schema_for_prompt
from multiAgents.state import AgentState
from multiAgents.config import DEBUG

def schema_analysis_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    스키마 분석 노드 - 사용자 입력을 분석하고 스키마 정보를 검색
    """
    if DEBUG:
        print("\n🔍 SCHEMA ANALYSIS NODE")
        print("="*30)
    
    # 사용자 입력 추출
    messages = state.get("messages", [])
    user_input = ""
    
    for msg in reversed(messages):
        if isinstance(msg, HumanMessage):
            user_input = msg.content
            break
    
    if not user_input:
        error_message = AIMessage(content="No user input found for schema analysis.")
        return {
            **state,
            "messages": messages + [error_message]
        }
    
    try:
        # 1. Clarifier 실행 (입력 분석)
        if DEBUG:
            print(f"🔍 Analyzing user input: {user_input[:100]}...")
        
        clarifier_result = clarifier_tool.invoke({"user_input": user_input})
        
        if not clarifier_result.get("success"):
            error_message = AIMessage(content="Failed to analyze user input for schema retrieval.")
            return {
                **state,
                "messages": messages + [error_message]
            }
        
        # 2. Schema 검색 실행
        if DEBUG:
            print("📊 Retrieving schema information...")
        
        schema_result = retrieve_schema_tool.invoke({
            "query": user_input,
            "similarity_threshold": 0.5
        })
        
        if not schema_result.get("success"):
            error_message = AIMessage(content=f"Schema retrieval failed: {schema_result.get('error', 'Unknown error')}")
            return {
                **state,
                "messages": messages + [error_message]
            }
        
        # 3. 결과 포맷팅
        formatted_result = format_schema_for_prompt(schema_result)
        
        result_message = AIMessage(content=formatted_result)
        
        if DEBUG:
            print("✅ Schema analysis completed successfully")
            print(f"Found {len(schema_result.get('tables', []))} relevant tables")
        
        return {
            **state,
            "messages": messages + [result_message]
        }
        
    except Exception as e:
        if DEBUG:
            print(f"❌ Schema analysis error: {str(e)}")
        
        error_message = AIMessage(content=f"Schema analysis encountered an error: {str(e)}")
        return {
            **state,
            "messages": messages + [error_message]
        }

# Schema Analyzer 그래프 생성
def create_schema_analyzer_graph():
    """Schema Analyzer 워크플로우 그래프 생성"""
    
    graph = StateGraph(AgentState)
    
    # 노드 추가
    graph.add_node("AnalyzeSchema", schema_analysis_node)
    
    # 워크플로우 정의
    graph.set_entry_point("AnalyzeSchema")
    graph.add_edge("AnalyzeSchema", END)
    
    return graph.compile()

# Schema Analyzer 그래프 컴파일
schema_analyzer_graph = create_schema_analyzer_graph()

def schema_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Schema Analyzer 메인 노드
    """
    if DEBUG:
        print("\n" + "="*50)
        print("🧠 SCHEMA ANALYZER STARTING")
        print("="*50)
    
    try:
        # 내부 그래프 실행
        result = schema_analyzer_graph.invoke(state)
        
        if DEBUG:
            print("="*50)
            print("🧠 SCHEMA ANALYZER COMPLETED")
            print("="*50)
        
        return result
        
    except Exception as e:
        if DEBUG:
            print(f"❌ Schema Analyzer Error: {str(e)}")
        
        error_message = AIMessage(content=f"Schema analysis failed: {str(e)}")
        return {
            **state,
            "messages": state.get("messages", []) + [error_message]
        }
