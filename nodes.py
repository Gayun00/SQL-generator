from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage
from state import ScheduleState
from config import LLM_CONFIG
import asyncio

llm = ChatOpenAI(
    model=LLM_CONFIG["model"],
    temperature=LLM_CONFIG["temperature"],
    max_tokens=LLM_CONFIG["max_tokens"]
)

async def clarifier(state: ScheduleState) -> ScheduleState:
    """사용자 입력이 유효한 일정 요청인지 판단"""
    print("🔍 Clarifier 노드 호출됨 - 사용자 입력 검증 중...")
    
    system_prompt = """
    사용자의 입력이 하루 일정 생성을 위한 유효한 요청인지 판단하세요.
    유효한 경우 'valid'를, 불명확하거나 부족한 경우 'invalid'를 반환하고 이유를 설명하세요.
    
    유효한 예시:
    - "내일 회사 일정 짜줘"
    - "오늘 운동과 공부를 포함한 일정 만들어줘"
    - "주말에 휴식과 취미활동이 포함된 스케줄 작성해줘"
    
    무효한 예시:
    - "안녕하세요"
    - "날씨가 어때?"
    - 너무 모호하거나 일정과 관련 없는 요청
    
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

async def wait_for_user(state: ScheduleState) -> ScheduleState:
    """사용자에게 재입력을 요청"""
    print("⏳ WaitForUser 노드 호출됨 - 사용자 재입력 대기 중...")
    
    feedback_message = f"❌ 입력이 유효하지 않습니다.\n💡 이유: {state.get('reason', '알 수 없는 오류')}\n✅ 하루 일정 생성을 위한 구체적인 요청을 다시 입력해주세요."
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
                    "finalOutput": "사용자 요청으로 일정 생성을 중단했습니다."
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

async def planner(state: ScheduleState) -> ScheduleState:
    """유효한 요청을 바탕으로 하루 일정 계획 수립"""
    print("📋 Planner 노드 호출됨 - 하루 일정 계획 수립 중...")
    
    system_prompt = """
    사용자의 요청을 바탕으로 하루 일정을 계획하세요.
    시간대별로 구체적이고 실현 가능한 일정을 만들어주세요.
    
    일정 형식:
    - 각 항목은 "시간: 활동내용" 형태
    - 현실적인 시간 배분
    - 휴식 시간 포함
    - 3-8개 정도의 주요 활동
    
    응답은 JSON 배열 형태로 각 일정 항목을 문자열로 반환하세요.
    예: ["08:00: 기상 및 아침 식사", "09:00: 업무 시작", ...]
    """
    
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=f"사용자 요청: {state['userInput']}")
    ]
    
    response = await llm.ainvoke(messages)
    
    # JSON 파싱 시도, 실패시 기본 일정으로 대체
    try:
        import json
        plan_list = json.loads(response.content)
    except:
        # 파싱 실패시 응답을 줄 단위로 분할
        plan_list = [line.strip() for line in response.content.split('\n') if line.strip()]
    
    return {
        **state,
        "plan": plan_list
    }

async def executor(state: ScheduleState) -> ScheduleState:
    """수립된 계획을 자연어 문장으로 요약"""
    print("⚡ Executor 노드 호출됨 - 일정을 자연어로 요약 중...")
    
    system_prompt = """
    다음 하루 일정을 자연스럽고 읽기 쉬운 문장으로 요약해주세요.
    친근하고 도움이 되는 톤으로 작성하며, 전체 일정의 흐름을 잘 설명해주세요.
    """
    
    plan_text = "\n".join(state.get("plan", []))
    
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=f"일정 목록:\n{plan_text}")
    ]
    
    response = await llm.ainvoke(messages)
    
    return {
        **state,
        "finalOutput": response.content
    }

async def orchestrator(state: ScheduleState) -> str:
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

async def final_answer(state: ScheduleState) -> ScheduleState:
    """최종 응답 출력"""
    print("✅ FinalAnswer 노드 호출됨 - 최종 응답 준비 완료!")
    print(f"🎉 최종 결과:\n{state.get('finalOutput', '일정 생성 완료')}")
    
    return state