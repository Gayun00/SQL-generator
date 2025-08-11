"""
User Communicator Agent - 사용자 입력 처리 및 의사소통
"""

from typing import Dict, Any, Optional
import re


class UserCommunicatorAgent:
    """사용자와의 의사소통을 담당하는 에이전트"""
    
    def __init__(self):
        """UserCommunicator Agent 초기화"""
        print("💬 UserCommunicator Agent 초기화")
    
    async def process_input(self, user_input: str) -> Dict[str, Any]:
        """
        사용자 입력을 처리하고 검증
        
        Args:
            user_input: 사용자 자연어 입력
            
        Returns:
            처리 결과
        """
        try:
            print(f"📥 사용자 입력 수신: {user_input}")
            
            # 입력 검증
            if not user_input or not user_input.strip():
                return {
                    "success": False,
                    "error": "빈 입력입니다. 질문을 입력해주세요.",
                    "processed_input": ""
                }
            
            # 입력 전처리
            processed_input = self._preprocess_input(user_input)
            
            # 입력 타입 분석
            input_type = self._analyze_input_type(processed_input)
            
            print(f"✅ 입력 처리 완료 - 타입: {input_type}")
            
            return {
                "success": True,
                "processed_input": processed_input,
                "original_input": user_input,
                "input_type": input_type,
                "message": "사용자 입력이 성공적으로 처리되었습니다."
            }
            
        except Exception as e:
            error_msg = f"사용자 입력 처리 중 오류: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                "success": False,
                "error": error_msg,
                "processed_input": ""
            }
    
    def _preprocess_input(self, user_input: str) -> str:
        """사용자 입력 전처리"""
        # 앞뒤 공백 제거
        processed = user_input.strip()
        
        # 연속된 공백을 단일 공백으로 변환
        processed = re.sub(r'\s+', ' ', processed)
        
        # 특수문자 정리 (필요시)
        # processed = re.sub(r'[^\w\s가-힣]', ' ', processed)
        
        return processed
    
    def _analyze_input_type(self, user_input: str) -> str:
        """사용자 입력 타입 분석"""
        input_lower = user_input.lower()
        
        # SQL 관련 키워드 체크
        sql_keywords = ['select', 'where', 'from', 'group by', 'order by', 'join']
        data_keywords = ['데이터', '조회', '검색', '찾', '보여', '알려', '분석']
        
        if any(keyword in input_lower for keyword in sql_keywords):
            return "sql_query"
        elif any(keyword in user_input for keyword in data_keywords):
            return "data_request"
        else:
            return "general_query"
    
    async def format_response(self, result: Dict[str, Any]) -> str:
        """
        결과를 사용자 친화적 형태로 포맷팅
        
        Args:
            result: 처리 결과
            
        Returns:
            포맷된 응답 문자열
        """
        try:
            if not result.get("success", False):
                return f"❌ 오류가 발생했습니다: {result.get('error', '알 수 없는 오류')}"
            
            response_parts = []
            
            # SQL 쿼리 포함
            if result.get("sql_query"):
                response_parts.append("📋 생성된 SQL 쿼리:")
                response_parts.append(f"```sql\n{result['sql_query']}\n```")
            
            # 실행 결과 포함
            if result.get("execution_result"):
                exec_result = result["execution_result"]
                if exec_result.get("success"):
                    row_count = exec_result.get("returned_rows", 0)
                    response_parts.append(f"✅ 쿼리 실행 완료: {row_count}개 결과")
                    
                    # 결과 데이터 일부 표시 (처음 3개 행)
                    results = exec_result.get("results", [])
                    if results:
                        response_parts.append("\n📊 결과 미리보기:")
                        for i, row in enumerate(results[:3]):
                            response_parts.append(f"Row {i+1}: {row}")
                        
                        if len(results) > 3:
                            response_parts.append(f"... (총 {len(results)}개 결과)")
                else:
                    response_parts.append(f"❌ 쿼리 실행 실패: {exec_result.get('error', '알 수 없는 오류')}")
            
            # 기본 메시지
            if result.get("message"):
                response_parts.append(f"\n💡 {result['message']}")
            
            return "\n".join(response_parts) if response_parts else "요청이 처리되었습니다."
            
        except Exception as e:
            return f"❌ 응답 포맷팅 중 오류: {str(e)}"
    
    def get_help_message(self) -> str:
        """도움말 메시지 반환"""
        return """
🤖 SQL Generator 도움말

📝 사용법:
  - 자연어로 원하는 데이터에 대해 질문하세요
  - 예: "지난달 매출 데이터를 보여줘", "고객별 주문 횟수를 알고 싶어"

💡 팁:
  - 구체적인 조건이나 기간을 명시하면 더 정확한 결과를 얻을 수 있습니다
  - 복잡한 질문은 단계별로 나누어 요청해보세요

❓ 예시 질문:
  - "최근 7일간 주문 데이터 조회"
  - "상품별 판매량 TOP 10"
  - "고객 연령대별 구매 패턴 분석"
        """


# 전역 UserCommunicator 인스턴스  
user_communicator_agent = UserCommunicatorAgent()