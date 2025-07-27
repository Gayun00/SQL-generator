"""
Schema Retriever - 사용자 쿼리를 기반으로 관련 스키마 검색
"""

from typing import List, Dict, Optional, Tuple
from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings
from langchain.schema import Document
from rag.schema_embedder import schema_embedder

class SchemaRetriever:
    def __init__(self, top_k: int = 5):
        """
        스키마 검색기 초기화
        
        Args:
            top_k: 검색할 상위 문서 수
        """
        self.top_k = top_k
        self.embeddings = OpenAIEmbeddings()
        self.vectorstore = None
        
    def initialize(self) -> bool:
        """검색기 초기화"""
        try:
            # schema_embedder의 벡터스토어 사용
            if not schema_embedder.vectorstore:
                if not schema_embedder.initialize_vectorstore():
                    return False
            
            self.vectorstore = schema_embedder.vectorstore
            print("✅ 스키마 검색기 초기화 완료")
            return True
            
        except Exception as e:
            print(f"❌ 스키마 검색기 초기화 실패: {str(e)}")
            return False
    
    def search_relevant_schemas(self, query: str, top_k: Optional[int] = None) -> List[Document]:
        """
        쿼리와 관련된 스키마 검색
        
        Args:
            query: 사용자 자연어 쿼리
            top_k: 검색할 문서 수
            
        Returns:
            관련 Document 리스트
        """
        if not self.vectorstore:
            print("❌ 벡터스토어가 초기화되지 않았습니다.")
            return []
        
        search_k = top_k or self.top_k
        
        try:
            print(f"🔍 쿼리 검색 중: '{query}'")
            
            # 유사도 검색 수행
            results = self.vectorstore.similarity_search_with_score(
                query=query,
                k=search_k
            )
            
            print(f"📊 검색 결과: {len(results)}개 문서 발견")
            
            # 결과 정리
            documents = []
            for doc, score in results:
                print(f"   - {doc.metadata.get('type', 'unknown')}: {doc.metadata.get('table_name', 'unknown')} (score: {score:.3f})")
                documents.append(doc)
            
            return documents
            
        except Exception as e:
            print(f"❌ 스키마 검색 실패: {str(e)}")
            return []
    
    def get_relevant_tables(self, query: str, top_k: Optional[int] = None) -> List[Dict]:
        """
        쿼리와 관련된 테이블 정보만 추출
        
        Args:
            query: 사용자 자연어 쿼리
            top_k: 검색할 문서 수
            
        Returns:
            테이블 정보 리스트
        """
        documents = self.search_relevant_schemas(query, top_k)
        
        # 테이블별로 그룹화
        tables = {}
        
        for doc in documents:
            table_name = doc.metadata.get('table_name')
            if not table_name:
                continue
            
            if table_name not in tables:
                tables[table_name] = {
                    'table_name': table_name,
                    'dataset': doc.metadata.get('dataset', ''),
                    'table_id': doc.metadata.get('table_id', ''),
                    'description': doc.metadata.get('description', ''),
                    'columns': [],
                    'relevance_score': 0,
                    'matched_elements': []
                }
            
            # 문서 타입에 따라 처리
            doc_type = doc.metadata.get('type')
            if doc_type == 'table':
                tables[table_name]['description'] = doc.metadata.get('description', '')
                tables[table_name]['matched_elements'].append('table_description')
            elif doc_type == 'column':
                column_info = {
                    'name': doc.metadata.get('column_name', ''),
                    'type': doc.metadata.get('column_type', ''),
                    'mode': doc.metadata.get('column_mode', ''),
                    'description': doc.metadata.get('column_description', '')
                }
                if column_info not in tables[table_name]['columns']:
                    tables[table_name]['columns'].append(column_info)
                    tables[table_name]['matched_elements'].append(f"column_{column_info['name']}")
        
        # 리스트로 변환 및 관련성 순으로 정렬
        table_list = list(tables.values())
        table_list.sort(key=lambda x: len(x['matched_elements']), reverse=True)
        
        return table_list
    
    def create_context_summary(self, query: str, max_tables: int = 3) -> str:
        """
        쿼리에 대한 컨텍스트 요약 생성
        
        Args:
            query: 사용자 자연어 쿼리
            max_tables: 포함할 최대 테이블 수
            
        Returns:
            컨텍스트 요약 문자열
        """
        relevant_tables = self.get_relevant_tables(query, top_k=10)
        
        if not relevant_tables:
            return "관련 스키마 정보를 찾을 수 없습니다."
        
        # 상위 테이블들만 선택
        selected_tables = relevant_tables[:max_tables]
        
        context_parts = []
        context_parts.append("=== 쿼리와 관련된 테이블 정보 ===\n")
        
        for i, table in enumerate(selected_tables, 1):
            context_parts.append(f"{i}. 테이블: {table['table_name']}")
            
            if table['description']:
                context_parts.append(f"   설명: {table['description']}")
            
            if table['columns']:
                context_parts.append("   주요 컬럼:")
                for col in table['columns'][:5]:  # 상위 5개 컬럼만
                    col_desc = f" - {col['description']}" if col['description'] else ""
                    context_parts.append(f"     • {col['name']} ({col['type']}, {col['mode']}){col_desc}")
                
                if len(table['columns']) > 5:
                    context_parts.append(f"     ... 및 {len(table['columns']) - 5}개 컬럼 더")
            
            context_parts.append("")  # 빈 줄
        
        return "\n".join(context_parts)
    
    def get_statistics(self) -> Dict:
        """검색기 통계 정보"""
        if not self.vectorstore:
            return {"status": "not_initialized"}
        
        try:
            collection_info = schema_embedder.get_collection_info()
            return {
                "status": "ready",
                "collection_name": collection_info.get("collection_name", "unknown"),
                "document_count": collection_info.get("document_count", 0),
                "persist_directory": collection_info.get("persist_directory", "unknown")
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}

# 전역 검색기 인스턴스
schema_retriever = SchemaRetriever()