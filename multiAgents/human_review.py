"""
Human Review Node for Human-in-the-Loop workflow
"""
from langchain_core.messages import HumanMessage
from multiAgents.config import AGENTS, TEST_MODE


def human_review_node(state) -> dict:
    """
    Human Review node that allows human intervention in the workflow.
    
    Args:
        state: Current conversation state with messages and next action
        
    Returns:
        dict: Contains approved next action or modified instruction
    """
    next_worker = state.get("next", "FINISH")
    
    if next_worker == "FINISH":
        print("\n🎯 Task is about to finish.")
        return {"next": next_worker}
    
    print("\n" + "="*50)
    print("🤔 HUMAN REVIEW CHECKPOINT")
    print("="*50)
    print(f"📍 Planned next action: Route to {next_worker}")
    
    if next_worker in AGENTS:
        print(f"📝 Description: {AGENTS[next_worker]['description']}")
        print(f"🔧 Capabilities:")
        for cap in AGENTS[next_worker].get('capabilities', []):
            print(f"   - {cap}")
    
    print("\nOptions:")
    print("1. ✅ Proceed with this action")
    print("2. 📝 Provide different instructions to supervisor")
    print("3. ❌ Skip this step and finish")
    
    if TEST_MODE:
        # 테스트 모드에서는 자동으로 choice 1 선택
        print("\n👤 Your choice (1/2/3): 1 (auto-selected in test mode)")
        choice = "1"
    else:
        while True:
            choice = input("\n👤 Your choice (1/2/3): ").strip()
            if choice in ["1", "2", "3"]:
                break
            print("❌ Invalid choice. Please enter 1, 2, or 3.")
    
    if choice == "1":
        # 그대로 진행
        print(f"✅ Approved: Proceeding to {next_worker}")
        return {"next": next_worker}
        
    elif choice == "2":
        # 사용자가 새로운 지시사항 제공
        if TEST_MODE:
            new_instruction = "Please get schema information first"  # 테스트용 기본값
            print(f"📝 Provide new instruction: {new_instruction} (auto-provided in test mode)")
        else:
            new_instruction = input("📝 Provide new instruction: ").strip()
            
        if new_instruction:
            # 새로운 지시사항을 메시지에 추가하고 Supervisor로 다시 라우팅
            new_messages = state.get("messages", []) + [HumanMessage(content=new_instruction)]
            print(f"📝 New instruction added: {new_instruction}")
            print("🔄 Routing back to Supervisor for re-evaluation")
            return {
                "messages": new_messages,
                "next": "Supervisor"
            }
        else:
            print("❌ No instruction provided. Defaulting to choice 1.")
            print(f"✅ Approved: Proceeding to {next_worker}")
            return {"next": next_worker}
            
    elif choice == "3":
        # 작업 종료
        print("❌ Task terminated by user")
        return {"next": "FINISH"}