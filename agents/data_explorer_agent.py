"""
DataExplorer Agent - 데이터 탐색 및 불확실성 해결 전문 Agent

기존 sql_explorer 노드를 Agent로 변환하여 데이터베이스 탐색,
불확실성 해결, 인사이트 발견에 특화된 지능형 Agent로 구현했습니다.
"""

from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import json
import re

from .base_agent import BaseAgent, AgentMessage, MessageType, AgentConfig, create_agent_config
from db.bigquery_client import bq_client
from langchain.schema import HumanMessage, SystemMessage

logger = logging.getLogger(__name__)

class ExplorationStrategy:
    """탐색 전략 분류"""
    QUICK_SCAN = "quick_scan"           # 빠른 스캔 (LIMIT 10)
    STATISTICAL = "statistical"        # 통계적 분석 
    RELATIONSHIP = "relationship"       # 테이블 간 관계 탐색
    VALUE_DISCOVERY = "value_discovery" # 컬럼 값 탐색
    TEMPORAL_ANALYSIS = "temporal"      # 시간 기반 분석

class DataExplorerAgent(BaseAgent):
    """데이터 탐색 및 불확실성 해결 전문 Agent"""
    
    def __init__(self, config: Optional[AgentConfig] = None):
        if config is None:
            config = create_agent_config(
                name="data_explorer",
                specialization="data_exploration_investigation",
                model="gpt-4",
                temperature=0.2,  # 탐색의 다양성과 안정성 균형
                max_tokens=1200
            )
        
        super().__init__(config)
        
        # 탐색 전용 설정
        self.exploration_strategies = {
            ExplorationStrategy.QUICK_SCAN: "빠른 데이터 스캔 및 구조 파악",
            ExplorationStrategy.STATISTICAL: "통계적 분석 및 집계",
            ExplorationStrategy.RELATIONSHIP: "테이블 간 관계 분석",
            ExplorationStrategy.VALUE_DISCOVERY: "컬럼 값 및 패턴 탐색",
            ExplorationStrategy.TEMPORAL_ANALYSIS: "시간 기반 데이터 분석"
        }
        
        # 성능 추적
        self.exploration_history = []
        self.investigation_stats = {
            "total_explorations": 0,
            "successful_explorations": 0,
            "insights_discovered": 0,
            "avg_exploration_time": 0.0,
            "uncertainty_resolution_rate": 0.0
        }
        
    
    def get_system_prompt(self) -> str:
        """DataExplorer 시스템 프롬프트"""
        return f"""
        당신은 BigQuery 데이터 탐색 전문가입니다.
        
        🚨 **핵심 규칙: BigQuery Standard SQL만 사용하세요!**
        
        **BigQuery 문법 규칙:**
        - 테이블명: `project.dataset.table` (백틱 필수)
        - LIMIT: LIMIT 20 (MySQL의 LIMIT offset, count 금지)
        - 스키마 조회: INFORMATION_SCHEMA 사용
        - 절대 금지: DESCRIBE, SHOW TABLES, SHOW COLUMNS 등 MySQL 문법
        
        **탐색 전략:**
        - 빠른 스캔: LIMIT 10-20 사용
        - 안전한 쿼리: 에러 방지를 위한 SAFE_CAST 등 사용
        - 효율적인 탐색: 불필요한 데이터 로드 방지
        
        **품질 목표:**
        - BigQuery 문법 준수율: 100%
        - 탐색 시간: 2-5초
        - 불확실성 해결률: 80% 이상
        """
    
    async def process_message(self, message: AgentMessage) -> AgentMessage:
        """메시지 처리 - 데이터 탐색 작업 수행"""
        try:
            # 입력 유효성 검증
            if not await self.validate_input(message):
                return self.create_error_message(message, ValueError("Invalid input message"))
            
            # 메시지 히스토리에 추가
            self.add_message_to_history(message)
            
            # 작업 타입에 따른 처리
            task_type = message.content.get("task_type", "uncertainty_exploration")
            input_data = message.content.get("input_data", {})
            
            if task_type == "uncertainty_exploration":
                result = await self._uncertainty_exploration(input_data)
            elif task_type == "data_discovery":
                result = await self._data_discovery(input_data)
            elif task_type == "relationship_analysis":
                result = await self._relationship_analysis(input_data)
            elif task_type == "statistical_analysis":
                result = await self._statistical_analysis(input_data)
            else:
                result = await self._uncertainty_exploration(input_data)  # 기본값
            
            # 성공 응답 생성
            return self.create_response_message(message, result)
            
        except Exception as e:
            logger.error(f"DataExplorer Agent processing failed: {str(e)}")
            return self.create_error_message(message, e)
    
    async def _uncertainty_exploration(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """불확실성 해결을 위한 탐색 - 표준 처리"""
        uncertainties = input_data.get("uncertainties", [])
        query = input_data.get("query", "")
        
        logger.info(f"DataExplorer: Uncertainty exploration for {len(uncertainties)} items")
        
        if not uncertainties:
            return {
                "exploration_type": "uncertainty_exploration",
                "executed_queries": 0,
                "results": [],
                "insights": [],
                "summary": "탐색할 불확실성이 없습니다.",
                "resolution_success": True
            }
        
        exploration_results = {
            "exploration_type": "uncertainty_exploration",
            "executed_queries": 0,
            "results": [],
            "insights": [],
            "summary": "",
            "resolution_success": False
        }
        
        start_time = datetime.now()
        successful_explorations = 0
        
        for i, uncertainty in enumerate(uncertainties, 1):
            logger.info(f"Processing uncertainty {i}/{len(uncertainties)}: {uncertainty.get('type', 'unknown')}")
            
            exploration_result = await self._explore_single_uncertainty(uncertainty, query)
            exploration_results["results"].append(exploration_result)
            
            if exploration_result["success"]:
                successful_explorations += 1
                exploration_results["executed_queries"] += 1
                
                # 인사이트 추가
                if exploration_result.get("insight"):
                    exploration_results["insights"].append(exploration_result["insight"])
        
        # 전체 결과 요약
        processing_time = (datetime.now() - start_time).total_seconds()
        exploration_results["summary"] = f"{successful_explorations}/{len(uncertainties)}개 불확실성 해결 완료"
        exploration_results["resolution_success"] = successful_explorations > 0
        exploration_results["processing_time"] = processing_time
        
        # 통계 업데이트
        self._update_exploration_stats(len(uncertainties), successful_explorations, processing_time)
        
        logger.info(f"Uncertainty exploration completed: {exploration_results['summary']}")
        return exploration_results
    
    async def _explore_single_uncertainty(self, uncertainty: Dict, original_query: str) -> Dict[str, Any]:
        """단일 불확실성 탐색"""
        uncertainty_type = uncertainty.get("type", "unknown")
        description = uncertainty.get("description", "")
        
        try:
            # 탐색 쿼리 자동 생성
            exploration_query = await self._generate_exploration_query(uncertainty, original_query)
            
            if not exploration_query:
                return {
                    "uncertainty_type": uncertainty_type,
                    "description": description,
                    "success": False,
                    "error": "탐색 쿼리 생성 실패",
                    "insight": f"{uncertainty_type} 불확실성을 해결할 탐색 쿼리를 생성할 수 없습니다."
                }
            
            # 탐색 쿼리 실행
            logger.info(f"Executing exploration query: {exploration_query[:100]}...")
            query_result = bq_client.execute_query(exploration_query, max_results=20)
            
            if query_result["success"]:
                # 결과 분석 및 인사이트 생성
                insight = await self._analyze_exploration_result(uncertainty, query_result)
                
                return {
                    "uncertainty_type": uncertainty_type,
                    "description": description,
                    "exploration_query": exploration_query,
                    "success": True,
                    "data": query_result["results"][:10],  # 상위 10개만 저장
                    "total_rows": query_result["total_rows"],
                    "returned_rows": query_result["returned_rows"],
                    "insight": insight,
                    "resolution_confidence": self._calculate_resolution_confidence(query_result)
                }
            else:
                return {
                    "uncertainty_type": uncertainty_type,
                    "description": description,
                    "exploration_query": exploration_query,
                    "success": False,
                    "error": query_result["error"],
                    "insight": f"탐색 실패로 {uncertainty_type} 불확실성을 해결할 수 없습니다."
                }
                
        except Exception as e:
            logger.error(f"Single uncertainty exploration failed: {str(e)}")
            return {
                "uncertainty_type": uncertainty_type,
                "description": description,
                "success": False,
                "error": str(e),
                "insight": f"오류로 인해 {uncertainty_type} 불확실성을 해결할 수 없습니다."
            }
    
    async def _generate_exploration_query(self, uncertainty: Dict, original_query: str) -> str:
        """불확실성에 따른 탐색 쿼리 생성"""
        uncertainty_type = uncertainty.get("type", "unknown")
        description = uncertainty.get("description", "")
        
        # 이미 제공된 탐색 쿼리가 있으면 사용
        if uncertainty.get("exploration_query"):
            return uncertainty["exploration_query"]
        
        # 데이터셋 경로 정보 추가
        dataset_info = ""
        if bq_client.full_dataset_path:
            dataset_info = f"""
        **데이터셋 경로 정보:**
        - 기본 경로: {bq_client.full_dataset_path}
        - 테이블 경로: `{bq_client.full_dataset_path}.table_name`
        - INFORMATION_SCHEMA: `{bq_client.full_dataset_path}.INFORMATION_SCHEMA.TABLES/COLUMNS`
        """
        
        # 자동 탐색 쿼리 생성
        prompt = f"""
    🚨 **중요: BigQuery Standard SQL만 사용하세요! MySQL/PostgreSQL 문법 절대 금지!**
    
    불확실성 정보:
    - 타입: {uncertainty_type}
    - 설명: {description}
    - 원본 쿼리: {original_query}
    
    {dataset_info}
    
    **BigQuery 탐색 쿼리 생성 규칙 (100% 준수 필수):**
    
    ✅ **허용되는 BigQuery 문법:**
    - SELECT, FROM, WHERE, GROUP BY, ORDER BY, LIMIT
    - INFORMATION_SCHEMA.TABLES, INFORMATION_SCHEMA.COLUMNS
    - 백틱으로 감싼 테이블명: `project.dataset.table`
    - LIMIT 20 (MySQL의 LIMIT offset, count 형식 금지)
    
    ❌ **절대 금지된 MySQL 문법:**
    - DESCRIBE table_name (→ SELECT * FROM INFORMATION_SCHEMA.COLUMNS 사용)
    - SHOW TABLES (→ SELECT table_name FROM INFORMATION_SCHEMA.TABLES 사용)
    - SHOW COLUMNS (→ SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS 사용)
    - 백틱 없는 테이블명
    - LIMIT offset, count 형식
    
    **불확실성 타입별 BigQuery 탐색 쿼리:**
    
    1. **column_values** (컬럼 값 탐색):
    ```sql
    SELECT column_name, COUNT(*) as count 
    FROM `{bq_client.full_dataset_path}.table_name` 
    GROUP BY column_name 
    ORDER BY count DESC 
    LIMIT 20
    ```
    
    2. **table_relationship** (테이블 관계 탐색):
    ```sql
    SELECT table_name, column_name 
    FROM `{bq_client.full_dataset_path}.INFORMATION_SCHEMA.COLUMNS` 
    WHERE table_name IN ('table1', 'table2') 
    ORDER BY table_name, column_name
    ```
    
    3. **data_range** (데이터 범위 확인):
    ```sql
    SELECT 
        MIN(column_name) as min_value,
        MAX(column_name) as max_value,
        COUNT(*) as total_count
    FROM `{bq_client.full_dataset_path}.table_name`
    ```
    
    4. **schema_ambiguity** (스키마 모호성 해결):
    ```sql
    SELECT table_name, column_name, data_type 
    FROM `{bq_client.full_dataset_path}.INFORMATION_SCHEMA.COLUMNS` 
    WHERE table_name = 'specific_table_name'
    ORDER BY ordinal_position
    ```
    
    **최종 검증 체크리스트:**
    - [ ] MySQL 문법 사용 안함 (DESCRIBE, SHOW 등)
    - [ ] 백틱으로 테이블명 감쌈
    - [ ] BigQuery Standard SQL만 사용
    - [ ] LIMIT 20 이하 사용
    - [ ] INFORMATION_SCHEMA 사용 (필요시)
    
    위 규칙을 100% 준수하여 BigQuery 탐색 쿼리를 생성해주세요.
    """
        
        try:
            response_content = await self.send_llm_request(prompt)
            return self._clean_sql_response(response_content)
        except Exception as e:
            logger.error(f"Exploration query generation failed: {str(e)}")
            return ""
    
    async def _analyze_exploration_result(self, uncertainty: Dict, query_result: Dict) -> str:
        """탐색 결과 분석 및 인사이트 생성"""
        uncertainty_type = uncertainty.get("type", "unknown")
        results = query_result.get("results", [])
        total_rows = query_result.get("total_rows", 0)
        
        if not results:
            return f"{uncertainty_type} 탐색 결과가 비어있습니다. 데이터가 존재하지 않거나 조건을 만족하는 레코드가 없습니다."
        
        # 결과 분석 프롬프트
        results_summary = json.dumps(results[:5], ensure_ascii=False, indent=2)  # 상위 5개만
        
        prompt = f"""
        탐색 결과 분석:
        - 불확실성 타입: {uncertainty_type}
        - 총 결과 수: {total_rows}
        - 샘플 데이터:
        {results_summary}
        
        이 탐색 결과를 바탕으로 다음을 제공해주세요:
        1. 발견된 주요 인사이트
        2. 불확실성 해결 방안
        3. SQL 생성 시 활용할 수 있는 구체적 정보
        
        간결하고 실용적인 한 문장으로 요약해주세요.
        """
        
        try:
            response_content = await self.send_llm_request(prompt)
            return response_content.strip()
        except Exception as e:
            logger.warning(f"Insight generation failed: {str(e)}")
            return f"{uncertainty_type} 탐색에서 {total_rows}개 결과를 발견했습니다."
    
    async def _data_discovery(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """일반적인 데이터 발견 탐색"""
        query = input_data.get("query", "")
        tables = input_data.get("tables", [])
        
        logger.info("DataExplorer: General data discovery started")
        
        discovery_results = {
            "exploration_type": "data_discovery",
            "discoveries": [],
            "table_insights": [],
            "recommendations": []
        }
        
        # 테이블별 기본 정보 탐색
        for table in tables[:3]:  # 최대 3개 테이블
            table_insight = await self._discover_table_structure(table)
            discovery_results["table_insights"].append(table_insight)
        
        return discovery_results
    
    async def _discover_table_structure(self, table_name: str) -> Dict[str, Any]:
        """테이블 구조 발견"""
        # BigQuery 문법으로 구조 탐색 쿼리 생성
        # 테이블명에 백틱 추가 (프로젝트.데이터셋 경로 포함)
        full_table_name = f"`us-all-data.us_plus.{table_name}`" if not table_name.startswith('`') else table_name
        
        structure_query = f"""
        SELECT 
            COUNT(*) as total_rows
        FROM {full_table_name}
        LIMIT 1
        """
        
        try:
            result = bq_client.execute_query(structure_query, max_results=5)
            if result["success"]:
                return {
                    "table": table_name,
                    "success": True,
                    "structure_info": result["results"],
                    "insight": f"{table_name} 테이블의 기본 구조를 파악했습니다."
                }
            else:
                return {
                    "table": table_name,
                    "success": False,
                    "error": result["error"]
                }
        except Exception as e:
            return {
                "table": table_name,
                "success": False,
                "error": str(e)
            }
    
    async def _relationship_analysis(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """테이블 간 관계 분석"""
        logger.info("DataExplorer: 테이블간 관계 분석")
        
        # 관계 분석 로직 구현
        return {
            "exploration_type": "relationship_analysis",
            "relationships": [],
            "join_recommendations": []
        }
    
    async def _statistical_analysis(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """통계적 분석"""
        logger.info("DataExplorer: 통계 분석")
        
        # 통계 분석 로직 구현
        return {
            "exploration_type": "statistical_analysis",
            "statistics": [],
            "trends": []
        }
    
    def _clean_sql_response(self, response_content: str) -> str:
        """SQL 응답 정리"""
        sql_query = response_content.strip()
        
        # 코드 블록 제거
        if sql_query.startswith("```sql"):
            sql_query = sql_query[6:]
        if sql_query.startswith("```"):
            sql_query = sql_query[3:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
        
        return sql_query.strip()
    
    def _calculate_resolution_confidence(self, query_result: Dict) -> float:
        """불확실성 해결 신뢰도 계산"""
        total_rows = query_result.get("total_rows", 0)
        returned_rows = query_result.get("returned_rows", 0)
        
        if total_rows == 0:
            return 0.3  # 데이터 없음
        elif returned_rows > 0:
            return min(0.9, 0.6 + (returned_rows / 20) * 0.3)  # 결과 있음
        else:
            return 0.5  # 결과 애매함
    
    def _update_exploration_stats(self, total_explorations: int, successful: int, processing_time: float):
        """탐색 통계 업데이트"""
        self.investigation_stats["total_explorations"] += total_explorations
        self.investigation_stats["successful_explorations"] += successful
        
        if successful > 0:
            self.investigation_stats["insights_discovered"] += successful
        
        # 평균 처리 시간 업데이트
        current_avg = self.investigation_stats["avg_exploration_time"]
        total_sessions = len(self.exploration_history) + 1
        self.investigation_stats["avg_exploration_time"] = (
            (current_avg * (total_sessions - 1) + processing_time) / total_sessions
        )
        
        # 해결률 업데이트
        if self.investigation_stats["total_explorations"] > 0:
            self.investigation_stats["uncertainty_resolution_rate"] = (
                self.investigation_stats["successful_explorations"] / 
                self.investigation_stats["total_explorations"]
            )
    
    def get_agent_statistics(self) -> Dict[str, Any]:
        """Agent 통계 정보 반환"""
        stats = self.investigation_stats
        
        if stats["total_explorations"] == 0:
            return {"message": "탐색 이력이 없습니다."}
        
        resolution_rate = stats["uncertainty_resolution_rate"] * 100
        
        return {
            "total_explorations": stats["total_explorations"],
            "successful_explorations": stats["successful_explorations"],
            "insights_discovered": stats["insights_discovered"],
            "resolution_rate": round(resolution_rate, 2),
            "avg_exploration_time": round(stats["avg_exploration_time"], 3),
            "performance_grade": "A" if resolution_rate > 80 and stats["avg_exploration_time"] < 5.0 else "B"
        }

def _validate_bigquery_syntax(self, sql_query: str) -> bool:
    """BigQuery 문법 검증"""
    sql_upper = sql_query.upper()
    
    # 금지된 MySQL 문법 검사
    forbidden_patterns = [
        r'\bDESCRIBE\b',
        r'\bSHOW\s+TABLES\b',
        r'\bSHOW\s+COLUMNS\b',
        r'\bUSE\b',
        r'\bCREATE\s+DATABASE\b'
    ]
    
    for pattern in forbidden_patterns:
        if re.search(pattern, sql_upper):
            return False
    
    # BigQuery 필수 요소 검사
    if not sql_upper.startswith('SELECT'):
        return False
    
    # 백틱이 있는지 확인 (테이블명)
    if '`' not in sql_query:
        return False
    
    return True

# Agent 생성 헬퍼 함수
def create_data_explorer_agent(custom_config: Optional[Dict[str, Any]] = None) -> DataExplorerAgent:
    """DataExplorer Agent 생성"""
    config = create_agent_config(
        name="data_explorer",
        specialization="data_exploration_investigation",
        model="gpt-4",
        temperature=0.2,
        max_tokens=1200,
        **(custom_config or {})
    )
    
    return DataExplorerAgent(config)