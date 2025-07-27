from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage
from workflow.state import SQLGeneratorState
from core.config import LLM_CONFIG
from db.bigquery_client import bq_client
import asyncio

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
    """유효한 요청을 바탕으로 SQL 쿼리 생성"""
    print("📋 SQLGenerator 노드 호출됨 - SQL 쿼리 생성 중...")
    
    # 스키마 정보 가져오기
    schema_summary = bq_client.get_schema_summary()
    
    system_prompt = f"""
    다음 BigQuery 스키마를 참고하여 사용자 요청에 맞는 SQL 쿼리를 생성하세요.
    
    {schema_summary}
    
    주의사항:
    - BigQuery 문법을 사용하세요
    - 테이블명은 완전한 형식 (dataset.table)으로 작성하세요
    - 효율적이고 성능이 좋은 쿼리를 생성하세요
    - 날짜 및 시간 처리에 주의하세요
    - LIMIT을 사용하여 결과를 제한하세요 (기본 100)
    
    SQL 쿼리만 반환하세요. 설명이나 다른 텍스트는 포함하지 마세요.
    """
    
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=f"사용자 요청: {state['userInput']}")
    ]
    
    response = await llm.ainvoke(messages)
    
    return {
        **state,
        "schemaInfo": bq_client.schema_info,
        "sqlQuery": response.content.strip()
    }

async def explainer(state: SQLGeneratorState) -> SQLGeneratorState:
    """생성된 SQL 쿼리에 대한 설명 생성"""
    print("⚡ Explainer 노드 호출됨 - SQL 쿼리 설명 생성 중...")
    
    system_prompt = """
    다음 SQL 쿼리에 대해 사용자가 이해하기 쉬운 설명을 생성해주세요.
    
    설명에 포함할 내용:
    1. 쿼리의 주요 목적
    2. 사용된 테이블과 컬럼
    3. 주요 로직 및 조건
    4. 예상되는 결과 형태
    
    친근하고 이해하기 쉬운 톤으로 작성해주세요.
    """
    
    sql_query = state.get("sqlQuery", "")
    
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=f"SQL 쿼리:\n{sql_query}")
    ]
    
    response = await llm.ainvoke(messages)
    
    # 최종 출력 구성
    final_output = f"""=== 생성된 SQL 쿼리 ===

```sql
{sql_query}
```

=== 쿼리 설명 ===
{response.content}"""
    
    return {
        **state,
        "explanation": response.content,
        "finalOutput": final_output
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
    
    # plan이 없음 → Planner
    if not state.get("plan"):
        print("➡️ 다음 노드: Planner (일정 계획 필요)")
        return "planner"
    
    # finalOutput이 없음 → Executor
    if not state.get("finalOutput"):
        print("➡️ 다음 노드: Executor (최종 요약 필요)")
        return "executor"
    
    # 모든 게 완료되면 → FinalAnswer
    print("➡️ 다음 노드: FinalAnswer (모든 단계 완료)")
    return "final_answer"

async def final_answer(state: SQLGeneratorState) -> SQLGeneratorState:
    """최종 응답 출력"""
    print("✅ FinalAnswer 노드 호출됨 - 최종 응답 준비 완료!")
    print(f"🎉 최종 결과:\n{state.get('finalOutput', 'SQL 생성 완료')}")
    
    return state