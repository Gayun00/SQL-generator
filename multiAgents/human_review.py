"""
간단한 Human Review Node - 디버깅용
"""
from multiAgents.config import TEST_MODE


def simple_human_review(step_name: str) -> bool:
    """
    간단한 human review - 계속 진행할지만 확인
    
    Args:
        step_name: 현재 실행할 단계 이름
        
    Returns:
        bool: True면 계속 진행, False면 종료
    """
    print("\n" + "="*40)
    print(f"🔍 STEP: {step_name}")
    print("="*40)
    print("1. ✅ Continue")
    print("2. ❌ Stop")
    
    if TEST_MODE:
        print("\n👤 Your choice (1/2): 1 (auto-selected in test mode)")
        choice = "1"
    else:
        while True:
            choice = input("\n👤 Your choice (1/2): ").strip()
            if choice in ["1", "2"]:
                break
            print("❌ Invalid choice. Please enter 1 or 2.")
    
    if choice == "1":
        print("✅ Continuing...")
        return True
    else:
        print("❌ Stopping execution")
        return False


def human_review_node(state) -> dict:
    """
    기존 human review node - 하위 호환성을 위해 유지
    """
    next_worker = state.get("next", "FINISH")
    
    if next_worker == "FINISH":
        return {"next": next_worker}
    
    if simple_human_review(f"Route to {next_worker}"):
        return {"next": next_worker}
    else:
        return {"next": "FINISH"}