"""
A2A (Agent-to-Agent) System for SQL Generator

중앙관리 방식의 Multi-Agent 시스템으로 SQL 생성의 각 단계를 
전문화된 Agent들이 협력하여 처리합니다.

주요 컴포넌트:
- BaseAgent: 모든 Agent의 기본 클래스
- MasterOrchestrator: 중앙 제어 및 조정
- Communication Hub: Agent 간 메시지 라우팅
- 전문 Agent들: SchemaIntelligence, QueryArchitect, DataInvestigator, CommunicationSpecialist
"""