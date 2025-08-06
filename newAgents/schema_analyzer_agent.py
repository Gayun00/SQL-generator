from typing import Dict, Any, List, Optional
import random

# Mock Schema Retriever (실제 rag/schema_retriever.py의 역할을 시뮬레이션)
class MockSchemaRetriever:
    async def get_relevant_tables_with_threshold(self, query: str, similarity_threshold: float) -> List[Dict]:
        print(f"[Schema Retriever] Searching for: '{query}' with threshold {similarity_threshold}")
        query_lower = query.lower()
        
        if "사용자" in query_lower or "고객" in query_lower or "user" in query_lower or "customers" in query_lower or "사람 정보" in query_lower:
            return [
                {"table_name": "users", "columns": ["id", "name", "email", "signup_date"], "relevance": 0.95},
                {"table_name": "user_profiles", "columns": ["user_id", "address", "phone"], "relevance": 0.85}
            ]
        elif "주문" in query_lower or "order" in query_lower:
            return [
                {"table_name": "orders", "columns": ["order_id", "user_id", "amount", "order_date"], "relevance": 0.95},
                {"table_name": "order_items", "columns": ["item_id", "order_id", "product_id", "quantity"], "relevance": 0.8}
            ]
        elif "매출" in query_lower or "sales" in query_lower:
            return [
                {"table_name": "sales_data", "columns": ["date", "revenue"], "relevance": 0.8},
                {"table_name": "transactions", "columns": ["trans_id", "amount"], "relevance": 0.75},
                {"table_name": "orders", "columns": ["order_id", "amount"], "relevance": 0.7}
            ]
        elif "알 수 없는 용어" in query_lower:
            return []
        else:
            if random.random() < 0.3:
                return []
            else:
                return [
                    {"table_name": "products", "columns": ["id", "name"], "relevance": 0.5},
                    {"table_name": "categories", "columns": ["id", "name"], "relevance": 0.4}
                ]

# Schema Analyzer Agent
class SchemaAnalyzer:
    def __init__(self):
        self.session_contexts = {}
        self.schema_retriever = MockSchemaRetriever()

    def _get_context(self, session_id: str) -> Dict[str, Any]:
        if session_id not in self.session_contexts:
            self.session_contexts[session_id] = {
                "last_query": None,
                "last_tables_found": None,
                "conversation_history": []
            }
        return self.session_contexts[session_id]

    def _is_sufficient_schema_info(self, tables: List[Dict], query: str) -> bool:
        print("[Schema Analyzer] Evaluating schema sufficiency...")
        if not tables:
            return False

        if len(tables) > 2 and ("매출" in query or "sales" in query):
            print("[Schema Analyzer] Too many tables found for ambiguous query.")
            return False

        avg_relevance = sum(t["relevance"] for t in tables) / len(tables)
        if avg_relevance < 0.7:
            print(f"[Schema Analyzer] Average relevance ({avg_relevance:.2f}) is too low.")
            return False

        print("[Schema Analyzer] Schema info seems sufficient.")
        return True

    def _generate_clarification_question(self, partial_tables: List[Dict], query: str) -> str:
        print("[Schema Analyzer] Generating clarification question...")
        if not partial_tables:
            return "어떤 데이터를 찾고 계신가요? 구체적인 테이블이나 데이터 종류를 알려주세요."
        
        if len(partial_tables) > 2:
            table_names = [t["table_name"] for t in partial_tables[:5]]
            return f"' {query} '와(과) 관련된 여러 테이블을 찾았습니다. 다음 중 어떤 것이 가장 적합한가요?\n옵션: {', '.join(table_names)}"
        
        return f"' {query} '에 대해 더 정확한 스키마 정보를 찾기 위해 추가 정보가 필요합니다. 어떤 종류의 데이터를 원하시나요?"

    def _format_schema_info(self, tables: List[Dict]) -> List[Dict]:
        formatted = []
        for table in tables:
            formatted.append({
                "table_name": table["table_name"],
                "columns": table["columns"]
            })
        return formatted

    async def process(self, message: Dict[str, Any]) -> Dict[str, Any]:
        session_id = message["session_id"]
        context = self._get_context(session_id)
        context["conversation_history"].append(message)

        query = message["content"]["query"]
        
        tables = await self.schema_retriever.get_relevant_tables_with_threshold(
            query,
            similarity_threshold=0.6
        )
        context["last_tables_found"] = tables
        context["last_query"] = query

        if self._is_sufficient_schema_info(tables, query):
            return {
                "status": "success",
                "schema_info": self._format_schema_info(tables),
                "next_agent": "sql_generator"
            }
        else:
            question = self._generate_clarification_question(tables, query)
            return {
                "status": "needs_user_clarification", 
                "question": question,
                "next_agent": "user_communicator"
            }