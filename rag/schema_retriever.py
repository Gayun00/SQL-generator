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
    

    def search_relevant_schemas_with_threshold(self, query: str, top_k: Optional[int] = None, similarity_threshold: float = 0.5) -> List[Document]:
        """
        유사도 임계값을 적용한 스키마 검색
        
        Args:
            query: 사용자 자연어 쿼리
            top_k: 검색할 문서 수
            similarity_threshold: 유사도 임계값 (0.0 ~ 1.0, 높을수록 엄격)
            
        Returns:
            임계값 이상의 유사도를 가진 Document 리스트
        """
        if not self.vectorstore:
            print("❌ 벡터스토어가 초기화되지 않았습니다.")
            return []
        
        search_k = top_k or self.top_k
        
        try:
            print(f"🔍 쿼리 검색 중 (임계값: {similarity_threshold}): '{query}'")
            
            # 유사도 검색 수행
            results = self.vectorstore.similarity_search_with_score(
                query=query,
                k=search_k
            )
            
            # 유사도 임계값 적용 필터링
            filtered_documents = []
            for doc, score in results:
                # ChromaDB는 distance를 반환하므로 similarity = 1 - distance로 계산
                # 단, distance가 이미 similarity일 수도 있으니 범위 확인
                if score <= 1.0:  # score가 distance인 경우
                    similarity = 1.0 - score
                else:  # score가 이미 similarity인 경우
                    similarity = score
                
                print(f"   - {doc.metadata.get('type', 'unknown')}: {doc.metadata.get('table_name', 'unknown')} (similarity: {similarity:.3f})")
                
                if similarity >= similarity_threshold:
                    filtered_documents.append(doc)
                else:
                    print(f"     → 임계값 미달로 제외 (required: {similarity_threshold})")
            
            print(f"📊 필터링 결과: {len(results)}개 중 {len(filtered_documents)}개 선택")
            
            return filtered_documents
            
        except Exception as e:
            print(f"❌ 스키마 검색 실패: {str(e)}")
            return []
    
    def get_relevant_tables_with_threshold(self, query: str, top_k: Optional[int] = None, similarity_threshold: float = 0.5) -> List[Dict]:
        """
        유사도 임계값을 적용한 관련 테이블 정보 추출
        
        Args:
            query: 사용자 자연어 쿼리
            top_k: 검색할 문서 수
            similarity_threshold: 유사도 임계값 (0.0 ~ 1.0, 높을수록 엄격)
            
        Returns:
            임계값 이상의 유사도를 가진 테이블 정보 리스트
        """
        documents = self.search_relevant_schemas_with_threshold(query, top_k, similarity_threshold)
        
        if not documents:
            return []
        
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
    

# 전역 검색기 인스턴스
schema_retriever = SchemaRetriever()