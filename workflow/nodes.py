from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage
from workflow.state import SQLGeneratorState
from core.config import LLM_CONFIG
from db.bigquery_client import bq_client
from rag.schema_retriever import schema_retriever
import asyncio
import json

llm = ChatOpenAI(
    model=LLM_CONFIG["model"],
    temperature=LLM_CONFIG["temperature"],
    max_tokens=LLM_CONFIG["max_tokens"]
)

async def clarifier(state: SQLGeneratorState) -> SQLGeneratorState:
    """사용자 입력이 유효한 SQL 쿼리 요청인지 판단"""
    print("🔍 Clarifier 노드 호출됨 - 사용자 입력 검증 중...")
    
    system_prompt = """
    사용자의 입력이 SQL 쿼리 생성을 위한 유효한 요청인지 판단하세요.
    유효한 경우 'valid'를, 불명확하거나 부족한 경우 'invalid'를 반환하고 이유를 설명하세요.
    
    유효한 예시:
    - "사용자별 주문 횟수를 조회해줘"
    - "지난달 매출 합계를 구하는 쿼리 만들어줘"
    - "상품별 재고량이 10개 미만인 데이터를 찾아줘"
    - "월별 신규 가입자 수 추이를 보여줘"
    
    무효한 예시:
    - "안녕하세요"
    - "날씨가 어때?"
    - 너무 모호하거나 데이터 조회와 관련 없는 요청
    
    응답 형식: "결과|이유"
    """
    
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=f"사용자 입력: {state['userInput']}")
    ]
    
    response = await llm.ainvoke(messages)
    result_parts = response.content.split('|')
    
    is_valid = result_parts[0].strip().lower() == 'valid'
    print(f"is_valid: {is_valid}")
    reason = result_parts[1].strip() if len(result_parts) > 1 else ""
    
    return {
        **state,
        "isValid": is_valid,
        "reason": reason
    }

async def wait_for_user(state: SQLGeneratorState) -> SQLGeneratorState:
    """사용자에게 재입력을 요청"""
    print("⏳ WaitForUser 노드 호출됨 - 사용자 재입력 대기 중...")
    
    feedback_message = f"❌ 입력이 유효하지 않습니다.\n💡 이유: {state.get('reason', '알 수 없는 오류')}\n✅ SQL 쿼리 생성을 위한 구체적인 데이터 조회 요청을 다시 입력해주세요."
    print(f"\n{feedback_message}")
    
    # 사용자에게 재입력 요청
    while True:
        try:
            new_input = input("\n🔄 다시 입력해주세요 (종료하려면 'quit' 입력): ").strip()
            
            if new_input.lower() in ['quit', 'exit', '종료']:
                return {
                    **state,
                    "userInput": new_input,
                    "isValid": False,
                    "reason": "사용자가 종료를 요청했습니다.",
                    "finalOutput": "사용자 요청으로 SQL 생성을 중단했습니다."
                }
            
            if not new_input:
                print("⚠️ 입력이 비어있습니다. 다시 입력해주세요.")
                continue
                
            # 새로운 입력으로 상태 업데이트 (validation은 clarifier에서 다시 수행)
            return {
                **state,
                "userInput": new_input,
                "isValid": False,  # clarifier에서 다시 검증하도록 False로 설정
                "reason": None
            }
            
        except KeyboardInterrupt:
            print("\n👋 사용자가 중단을 요청했습니다.")
            return {
                **state,
                "userInput": "quit",
                "isValid": False,
                "reason": "사용자가 중단을 요청했습니다.",
                "finalOutput": "사용자 요청으로 일정 생성을 중단했습니다."
            }
        except Exception as e:
            print(f"❌ 입력 처리 중 오류가 발생했습니다: {str(e)}")
            continue

async def sql_generator(state: SQLGeneratorState) -> SQLGeneratorState:
    """유효한 요청을 바탕으로 SQL 쿼리 생성 (RAG 기반 + 탐색 결과 활용)"""
    print("📋 SQLGenerator 노드 호출됨 - SQL 쿼리 생성 중...")
    
    user_query = state['userInput']
    
    # RAG를 통한 관련 스키마 검색
    print("🔍 RAG 기반 관련 스키마 검색 중...")
    relevant_context = schema_retriever.create_context_summary(user_query, max_tables=5)
    
    # 탐색 결과가 있으면 추가 컨텍스트로 활용
    exploration_context = ""
    exploration_results = state.get("explorationResults")
    if exploration_results and exploration_results.get("insights"):
        print("💡 탐색 결과를 SQL 생성에 활용 중...")
        insights = exploration_results.get("insights", [])
        exploration_context = f"""
        
=== 탐색을 통해 발견된 정보 ===
{chr(10).join([f"- {insight}" for insight in insights])}

이 정보를 바탕으로 더 정확한 SQL 쿼리를 생성하세요.
        """
    
    system_prompt = f"""
    사용자의 요청을 분석하여 BigQuery SQL 쿼리를 생성하세요.
    
    다음 관련 스키마 정보를 참고하세요:
    {relevant_context}
    {exploration_context}
    
    주의사항:
    - BigQuery 문법을 사용하세요
    - 테이블명은 완전한 형식 (dataset.table)으로 작성하세요
    - 효율적이고 성능이 좋은 쿼리를 생성하세요
    - 날짜 및 시간 처리에 주의하세요 (TIMESTAMP, DATE 함수 활용)
    - LIMIT을 사용하여 결과를 제한하세요 (기본 100)
    - JOIN이 필요한 경우 적절한 JOIN 조건을 사용하세요
    - 집계 함수나 윈도우 함수가 필요한 경우 적절히 활용하세요
    - 탐색 결과에서 발견된 정보를 정확히 반영하세요
    
    SQL 쿼리만 반환하세요. 설명이나 다른 텍스트는 포함하지 마세요.
    """
    
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=f"사용자 요청: {user_query}")
    ]
    
    response = await llm.ainvoke(messages)
    
    # SQL 쿼리 정리 (```sql ... ``` 형태 제거)
    sql_query = response.content.strip()
    
    # 코드 블록 제거
    if sql_query.startswith("```sql"):
        sql_query = sql_query[6:]  # ```sql 제거
    if sql_query.startswith("```"):
        sql_query = sql_query[3:]   # ``` 제거
    if sql_query.endswith("```"):
        sql_query = sql_query[:-3]  # 끝의 ``` 제거
    
    sql_query = sql_query.strip()
    
    return {
        **state,
        "schemaInfo": bq_client.schema_info,
        "sqlQuery": sql_query
    }

async def explainer(state: SQLGeneratorState) -> SQLGeneratorState:
    """생성된 SQL 쿼리에 대한 설명 생성"""
    print("⚡ Explainer 노드 호출됨 - SQL 쿼리 설명 생성 중...")
    
    sql_query = state.get("sqlQuery", "")
    query_results = state.get("queryResults", {})
    execution_status = state.get("executionStatus", "unknown")
    
    # 실행 결과에 따라 다른 설명 생성
    if execution_status == "success" and query_results.get("success"):
        system_prompt = """
        다음 SQL 쿼리와 실행 결과에 대해 사용자가 이해하기 쉬운 설명을 생성해주세요.
        
        설명에 포함할 내용:
        1. 쿼리의 주요 목적
        2. 사용된 테이블과 컬럼
        3. 주요 로직 및 조건
        4. 실행 결과 요약 (행 수, 주요 특징 등)
        
        친근하고 이해하기 쉬운 톤으로 작성해주세요.
        """
        
        # 실행 결과 요약
        results_summary = f"""
실행 결과:
- 반환된 행 수: {query_results.get('returned_rows', 0)}개
- 전체 행 수: {query_results.get('total_rows', 0)}개
- 처리된 데이터: {query_results.get('bytes_processed', 0):,} bytes
"""
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"SQL 쿼리:\n{sql_query}\n{results_summary}")
        ]
    else:
        system_prompt = """
        다음 SQL 쿼리에 대해 설명을 생성해주세요. 쿼리 실행에 실패했으므로 쿼리 자체에 대한 설명과 실패 원인에 대한 분석을 포함해주세요.
        
        설명에 포함할 내용:
        1. 쿼리의 의도된 목적
        2. 사용하려던 테이블과 컬럼
        3. 실행 실패 원인 분석
        4. 개선 방안 제안
        """
        
        error_info = f"실행 실패 정보: {query_results.get('error', '알 수 없는 오류')}"
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"SQL 쿼리:\n{sql_query}\n\n{error_info}")
        ]
    
    response = await llm.ainvoke(messages)
    
    # 최종 출력 구성
    final_output = f"""=== 생성된 SQL 쿼리 ===

```sql
{sql_query}
```

=== 실행 결과 ===
"""
    
    if execution_status == "success" and query_results.get("success"):
        final_output += f"""✅ 쿼리 실행 성공!
📊 반환된 결과: {query_results.get('returned_rows', 0)}개 행
📈 전체 데이터: {query_results.get('total_rows', 0)}개 행
💾 처리된 데이터: {query_results.get('bytes_processed', 0):,} bytes

=== 결과 데이터 (상위 5개) ===
"""
        # 상위 5개 결과 표시
        results = query_results.get('results', [])
        for i, row in enumerate(results[:5]):
            final_output += f"\n{i+1}. {row}"
        
        if len(results) > 5:
            final_output += f"\n... (총 {len(results)}개 중 5개만 표시)"
    else:
        final_output += f"""❌ 쿼리 실행 실패
오류: {query_results.get('error', '알 수 없는 오류')}
제안: {query_results.get('suggestion', '쿼리를 다시 확인해보세요.')}
"""
    
    final_output += f"""

=== 쿼리 설명 ===
{response.content}"""
    
    return {
        **state,
        "explanation": response.content,
        "finalOutput": final_output
    }

async def sql_executor(state: SQLGeneratorState) -> SQLGeneratorState:
    """생성된 SQL 쿼리를 실제 BigQuery에서 실행"""
    print("⚡ SQLExecutor 노드 호출됨 - SQL 쿼리 실행 중...")
    
    sql_query = state.get("sqlQuery", "")
    if not sql_query:
        return {
            **state,
            "executionStatus": "failed",
            "queryResults": {
                "success": False,
                "error": "실행할 SQL 쿼리가 없습니다.",
                "results": []
            }
        }
    
    try:
        # BigQuery에서 SQL 실행
        print(f"🔍 실행할 쿼리:\n{sql_query}")
        results = bq_client.execute_query(sql_query, max_results=50)
        
        if results["success"]:
            print(f"✅ 쿼리 실행 성공! {results['returned_rows']}개 결과 반환")
            execution_status = "success"
        else:
            print(f"❌ 쿼리 실행 실패: {results['error']}")
            execution_status = "failed"
        
        return {
            **state,
            "executionStatus": execution_status,
            "queryResults": results
        }
        
    except Exception as e:
        error_msg = f"쿼리 실행 중 예상치 못한 오류: {str(e)}"
        print(f"❌ {error_msg}")
        
        return {
            **state,
            "executionStatus": "failed",
            "queryResults": {
                "success": False,
                "error": error_msg,
                "results": []
            }
        }

async def orchestrator(state: SQLGeneratorState) -> str:
    """현재 상태에 따라 다음 노드를 결정"""
    print("🎯 Orchestrator 노드 호출됨 - 다음 단계 결정 중...")
    print(f"현재 상태: isValid={state.get('isValid')}, userInput='{state.get('userInput')}', reason='{state.get('reason')}'")
    
    # 사용자가 종료를 요청한 경우 → FinalAnswer
    user_input = state.get("userInput", "").lower()
    if user_input in ['quit', 'exit', '종료']:
        print("➡️ 다음 노드: FinalAnswer (사용자 종료 요청)")
        return "final_answer"
    
    # wait_for_user에서 새로운 입력이 들어온 경우 → Clarifier로 재검증
    if state.get("reason") is None and not state.get("isValid", True):
        print("➡️ 다음 노드: Clarifier (새 입력 재검증 필요)")
        return "clarifier"
    
    # isValid가 false → WaitForUser (단, 이미 finalOutput이 있다면 종료)
    if not state.get("isValid", True):
        if state.get("finalOutput"):  # 종료 메시지가 이미 설정된 경우
            print("➡️ 다음 노드: FinalAnswer (종료 처리 완료)")
            return "final_answer"
        print("➡️ 다음 노드: WaitForUser (입력이 유효하지 않음)")
        return "wait_for_user"
    
    # 불확실성 분석이 없음 → SQLAnalyzer  
    if not state.get("uncertaintyAnalysis"):
        print("➡️ 다음 노드: SQLAnalyzer (불확실성 분석 필요)")
        return "sql_analyzer"
    
    # 불확실성이 존재하고 탐색 결과가 없음 → SQLExplorer
    if state.get("hasUncertainty") and not state.get("explorationResults"):
        print("➡️ 다음 노드: SQLExplorer (탐색 쿼리 실행 필요)")
        return "sql_explorer"
    
    # SQL 쿼리가 없음 → SQLGenerator
    if not state.get("sqlQuery"):
        print("➡️ 다음 노드: SQLGenerator (SQL 쿼리 생성 필요)")
        return "sql_generator"
    
    # SQL 실행 결과가 없음 → SQLExecutor  
    if not state.get("queryResults"):
        print("➡️ 다음 노드: SQLExecutor (SQL 실행 필요)")
        return "sql_executor"
    
    # 설명이 없음 → Explainer
    if not state.get("explanation"):
        print("➡️ 다음 노드: Explainer (설명 생성 필요)")
        return "explainer"
    
    # 모든 게 완료되면 → FinalAnswer
    print("➡️ 다음 노드: FinalAnswer (모든 단계 완료)")
    return "final_answer"

async def sql_analyzer(state: SQLGeneratorState) -> SQLGeneratorState:
    """사용자 쿼리의 불확실한 요소 분석"""
    print("🔍 SQLAnalyzer 노드 호출됨 - 쿼리 불확실성 분석 중...")
    
    user_query = state['userInput']
    
    # RAG를 통한 관련 스키마 검색
    print("📋 RAG 기반 관련 스키마 검색 중...")
    relevant_context = schema_retriever.create_context_summary(user_query, max_tables=5)
    
    system_prompt = f"""
    사용자의 SQL 요청을 분석하여 불확실한 요소들을 식별하세요.
    
    다음 관련 스키마 정보를 참고하세요:
    {relevant_context}
    
    불확실성 유형:
    1. column_values: 컬럼에 어떤 값들이 있는지 모르는 경우
       - 예: "상태가 '활성'인 사용자" → status 컬럼에 정확히 어떤 값들이 있는지 확인 필요
       - 예: "카테고리별 매출" → category 컬럼의 실제 값들 확인 필요
    
    2. table_relationship: 테이블 간 관계가 불분명한 경우
       - 예: "사용자별 주문 정보" → users와 orders 테이블의 연결 방법
       - 예: "상품과 주문의 관계" → 중간 테이블 존재 여부
    
    3. data_range: 데이터의 범위나 분포가 불분명한 경우
       - 예: "최근 데이터" → 실제 데이터의 날짜 범위
       - 예: "인기 상품" → 판매량이나 평점의 기준값
    
    응답 형식 (JSON):
    {{
        "has_uncertainty": true/false,
        "uncertainties": [
            {{
                "type": "column_values|table_relationship|data_range",
                "description": "불확실성 설명",
                "table": "관련 테이블명",
                "column": "관련 컬럼명 (해당시)",
                "exploration_query": "탐지를 위한 SQL 쿼리"
            }}
        ],
        "confidence": 0.0-1.0
    }}
    
    사용자 요청을 신중히 분석하여 정확한 SQL 생성을 위해 추가 정보가 필요한 부분을 찾아주세요.
    """
    
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=f"사용자 요청: {user_query}")
    ]
    
    response = await llm.ainvoke(messages)
    
    try:
        # JSON 응답 파싱 (코드 블록 제거)
        response_content = response.content.strip()
        
        # ```json ... ``` 형태의 코드 블록 제거
        if response_content.startswith("```json"):
            response_content = response_content[7:]  # ```json 제거
        if response_content.startswith("```"):
            response_content = response_content[3:]   # ``` 제거
        if response_content.endswith("```"):
            response_content = response_content[:-3]  # 끝의 ``` 제거
        
        response_content = response_content.strip()
        
        analysis_result = json.loads(response_content)
        
        print(f"📊 불확실성 분석 완료:")
        print(f"   - 불확실성 존재: {analysis_result.get('has_uncertainty', False)}")
        print(f"   - 신뢰도: {analysis_result.get('confidence', 0.0):.2f}")
        
        uncertainties = analysis_result.get('uncertainties', [])
        if uncertainties:
            print(f"   - 발견된 불확실성: {len(uncertainties)}개")
            for i, uncertainty in enumerate(uncertainties, 1):
                print(f"     {i}. {uncertainty.get('type', 'unknown')}: {uncertainty.get('description', 'N/A')}")
        
        return {
            **state,
            "uncertaintyAnalysis": analysis_result,
            "hasUncertainty": analysis_result.get('has_uncertainty', False)
        }
        
    except json.JSONDecodeError as e:
        print(f"⚠️ JSON 파싱 실패: {e}")
        print(f"원본 응답: {response.content}")
        
        # 파싱 실패시 기본값 반환
        return {
            **state,
            "uncertaintyAnalysis": {
                "has_uncertainty": False,
                "uncertainties": [],
                "confidence": 0.0,
                "error": "JSON 파싱 실패"
            },
            "hasUncertainty": False
        }
    
    except Exception as e:
        print(f"❌ 불확실성 분석 중 오류: {str(e)}")
        
        return {
            **state,
            "uncertaintyAnalysis": {
                "has_uncertainty": False,
                "uncertainties": [],
                "confidence": 0.0,
                "error": str(e)
            },
            "hasUncertainty": False
        }

async def sql_explorer(state: SQLGeneratorState) -> SQLGeneratorState:
    """불확실성 해결을 위한 탐색 쿼리 실행"""
    print("🔍 SQLExplorer 노드 호출됨 - 탐색 쿼리 실행 중...")
    
    uncertainty_analysis = state.get("uncertaintyAnalysis", {})
    uncertainties = uncertainty_analysis.get("uncertainties", [])
    
    if not uncertainties:
        print("⚠️ 실행할 탐색 쿼리가 없습니다.")
        return {
            **state,
            "explorationResults": {
                "executed_queries": 0,
                "results": [],
                "summary": "탐색할 불확실성이 없습니다."
            }
        }
    
    exploration_results = {
        "executed_queries": 0,
        "results": [],
        "summary": "",
        "insights": []
    }
    
    print(f"📊 {len(uncertainties)}개의 불확실성에 대한 탐색 쿼리 실행 중...")
    
    for i, uncertainty in enumerate(uncertainties, 1):
        uncertainty_type = uncertainty.get("type", "unknown")
        description = uncertainty.get("description", "N/A")
        exploration_query = uncertainty.get("exploration_query", "")
        
        print(f"\n🔍 탐색 {i}/{len(uncertainties)}: {uncertainty_type}")
        print(f"   설명: {description}")
        
        if not exploration_query:
            print(f"   ⚠️ 탐색 쿼리가 제공되지 않았습니다.")
            continue
            
        print(f"   쿼리: {exploration_query}")
        
        try:
            # 탐색 쿼리 실행 (결과를 제한하여 빠른 실행)
            query_result = bq_client.execute_query(exploration_query, max_results=20)
            
            exploration_results["executed_queries"] += 1
            
            if query_result["success"]:
                print(f"   ✅ 탐색 성공: {query_result['returned_rows']}개 결과")
                
                # 결과 분석 및 인사이트 생성
                insight = await analyze_exploration_result(uncertainty, query_result)
                
                exploration_results["results"].append({
                    "uncertainty_type": uncertainty_type,
                    "description": description,
                    "query": exploration_query,
                    "success": True,
                    "data": query_result["results"][:10],  # 상위 10개만 저장
                    "total_rows": query_result["total_rows"],
                    "insight": insight
                })
                
                exploration_results["insights"].append(insight)
                print(f"   💡 인사이트: {insight}")
                
            else:
                print(f"   ❌ 탐색 실패: {query_result['error']}")
                exploration_results["results"].append({
                    "uncertainty_type": uncertainty_type,
                    "description": description,
                    "query": exploration_query,
                    "success": False,
                    "error": query_result["error"],
                    "insight": f"탐색 실패로 {uncertainty_type} 불확실성을 해결할 수 없습니다."
                })
                
        except Exception as e:
            error_msg = f"탐색 쿼리 실행 중 오류: {str(e)}"
            print(f"   💥 {error_msg}")
            
            exploration_results["results"].append({
                "uncertainty_type": uncertainty_type,
                "description": description,
                "query": exploration_query,
                "success": False,
                "error": error_msg,
                "insight": f"오류로 인해 {uncertainty_type} 불확실성을 해결할 수 없습니다."
            })
    
    # 전체 탐색 결과 요약 생성
    successful_explorations = len([r for r in exploration_results["results"] if r["success"]])
    total_explorations = len(exploration_results["results"])
    
    exploration_results["summary"] = f"{successful_explorations}/{total_explorations}개 탐색 완료"
    
    print(f"\n📋 탐색 완료: {exploration_results['summary']}")
    if exploration_results["insights"]:
        print("💡 주요 인사이트:")
        for insight in exploration_results["insights"]:
            print(f"   - {insight}")
    
    return {
        **state,
        "explorationResults": exploration_results
    }

async def analyze_exploration_result(uncertainty: dict, query_result: dict) -> str:
    """탐색 결과를 분석하여 인사이트 생성"""
    uncertainty_type = uncertainty.get("type", "unknown")
    results = query_result.get("results", [])
    total_rows = query_result.get("total_rows", 0)
    
    if not results:
        return f"{uncertainty_type} 탐색 결과가 비어있습니다."
    
    try:
        if uncertainty_type == "column_values":
            # 컬럼 값 분석
            if len(results) == 1 and len(results[0]) == 1:
                # DISTINCT 값들 조회인 경우
                column_name = list(results[0].keys())[0]
                unique_values = [str(row[column_name]) for row in results if row[column_name] is not None]
                if len(unique_values) <= 5:
                    return f"가능한 값: {', '.join(unique_values)}"
                else:
                    return f"총 {len(unique_values)}개의 고유 값 발견 (예: {', '.join(unique_values[:3])}, ...)"
            else:
                return f"컬럼 값 탐색 완료: {total_rows}개 행, 샘플 데이터 확인됨"
                
        elif uncertainty_type == "table_relationship":
            # 테이블 관계 분석
            if results:
                sample_keys = list(results[0].keys())
                return f"연결 키 확인: {', '.join(sample_keys)} ({total_rows}개 관계 발견)"
            else:
                return "테이블 간 관계를 확인할 수 없습니다."
                
        elif uncertainty_type == "data_range":
            # 데이터 범위 분석
            if len(results) >= 1:
                first_row = results[0]
                if 'min' in str(first_row).lower() and 'max' in str(first_row).lower():
                    # MIN/MAX 쿼리 결과인 경우
                    return f"데이터 범위 확인: {first_row}"
                else:
                    return f"데이터 범위 탐색 완료: {total_rows}개 레코드 분석"
            else:
                return "데이터 범위를 확인할 수 없습니다."
        
        else:
            return f"{uncertainty_type} 탐색 완료: {total_rows}개 결과"
            
    except Exception as e:
        return f"결과 분석 중 오류: {str(e)}"

async def final_answer(state: SQLGeneratorState) -> SQLGeneratorState:
    """최종 응답 출력"""
    print("✅ FinalAnswer 노드 호출됨 - 최종 응답 준비 완료!")
    print(f"🎉 최종 결과:\n{state.get('finalOutput', 'SQL 생성 완료')}")
    
    return state