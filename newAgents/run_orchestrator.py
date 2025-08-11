import asyncio
import logging
import json
from .orchestrator_agent import OrchestratorAgent

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def main():
    """
    OrchestratorAgent를 사용하여 사용자 요청을 대화형으로 처리하는 메인 함수
    """
    # OrchestratorAgent 인스턴스 생성
    print("🤖 SQL 생성 에이전트가 준비되었습니다. 질문을 입력해주세요.")
    agent = OrchestratorAgent()
    
    while True:
        try:
            # 사용자로부터 직접 입력 받기
            user_query = input("\n> 질문을 입력하세요 (종료하려면 'exit' 또는 'q' 입력): ")
            
            if user_query.lower() in ['exit', 'q']:
                print("\n👋 프로그램을 종료합니다.")
                break
                
            if not user_query.strip():
                print("입력 내용이 없습니다. 다시 시도해주세요.")
                continue

            logging.info(f"사용자 요청: \"{user_query}\"")
            
            # 에이전트의 process_request 메소드 호출
            result = await agent.process_request(user_query)
            
            # 결과 출력
            logging.info("최종 처리 결과:")
            print(json.dumps(result, indent=2, ensure_ascii=False))
            print("\n" + "="*70)

        except KeyboardInterrupt:
            print("\n\n👋 프로그램을 종료합니다.")
            break
        except Exception as e:
            logging.error(f"처리 중 오류 발생: {e}")

if __name__ == "__main__":
    asyncio.run(main())