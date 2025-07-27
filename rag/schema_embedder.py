"""
Schema Embedder - BigQuery 스키마 정보를 ChromaDB에 임베딩
"""

import os
import json
from typing import Dict, List, Optional
from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings
from langchain.schema import Document
from core.config import LLM_CONFIG

class SchemaEmbedder:
    def __init__(self, persist_directory: str = "./chroma_db"):
        """
        스키마 임베딩 초기화
        
        Args:
            persist_directory: ChromaDB 저장 디렉토리
        """
        self.persist_directory = persist_directory
        self.embeddings = OpenAIEmbeddings()
        self.vectorstore = None
        self.collection_name = "bigquery_schemas"
        
    def initialize_vectorstore(self):
        """벡터스토어 초기화"""
        try:
            self.vectorstore = Chroma(
                collection_name=self.collection_name,
                embedding_function=self.embeddings,
                persist_directory=self.persist_directory
            )
            print(f"✅ ChromaDB 벡터스토어 초기화 완료: {self.persist_directory}")
            return True
        except Exception as e:
            print(f"❌ ChromaDB 초기화 실패: {str(e)}")
            return False
    
    def create_table_documents(self, schema_info: Dict) -> List[Document]:
        """
        스키마 정보를 Document 형태로 변환
        
        Args:
            schema_info: BigQuery 스키마 정보
            
        Returns:
            Document 리스트
        """
        documents = []
        
        for table_name, schema in schema_info.items():
            # 테이블 기본 정보 문서
            table_doc_content = f"""테이블: {table_name}
설명: {schema.get('description', '설명 없음')}
컬럼 수: {len(schema.get('columns', []))}

컬럼 정보:"""
            
            # 컬럼 정보 추가
            for col in schema.get('columns', []):
                col_desc = col.get('description', '')
                table_doc_content += f"""
- {col['name']} ({col['type']}, {col['mode']})"""
                if col_desc:
                    table_doc_content += f": {col_desc}"
            
            # 테이블 문서 생성
            table_document = Document(
                page_content=table_doc_content,
                metadata={
                    "type": "table",
                    "table_name": table_name,
                    "dataset": table_name.split('.')[0] if '.' in table_name else '',
                    "table_id": table_name.split('.')[1] if '.' in table_name else table_name,
                    "column_count": len(schema.get('columns', [])),
                    "description": schema.get('description', '')
                }
            )
            documents.append(table_document)
            
            # 각 컬럼을 개별 문서로도 생성 (더 세밀한 검색을 위해)
            for col in schema.get('columns', []):
                col_content = f"""테이블: {table_name}
컬럼: {col['name']}
타입: {col['type']}
모드: {col['mode']}
설명: {col.get('description', '설명 없음')}

이 컬럼은 {table_name} 테이블의 {col['type']} 타입 필드입니다."""
                
                col_document = Document(
                    page_content=col_content,
                    metadata={
                        "type": "column",
                        "table_name": table_name,
                        "column_name": col['name'],
                        "column_type": col['type'],
                        "column_mode": col['mode'],
                        "column_description": col.get('description', ''),
                        "dataset": table_name.split('.')[0] if '.' in table_name else '',
                        "table_id": table_name.split('.')[1] if '.' in table_name else table_name
                    }
                )
                documents.append(col_document)
        
        return documents
    
    def embed_schemas(self, schema_info: Dict) -> bool:
        """
        스키마 정보를 벡터스토어에 임베딩
        
        Args:
            schema_info: BigQuery 스키마 정보
            
        Returns:
            성공 여부
        """
        if not self.vectorstore:
            print("❌ 벡터스토어가 초기화되지 않았습니다.")
            return False
        
        try:
            print("🔍 스키마 문서 생성 중...")
            documents = self.create_table_documents(schema_info)
            print(f"📝 생성된 문서 수: {len(documents)}개")
            
            # 기존 컬렉션 초기화 (새로운 스키마로 업데이트)
            print("🗑️ 기존 임베딩 데이터 삭제 중...")
            try:
                # 기존 컬렉션의 모든 문서 삭제
                collection = self.vectorstore._collection
                if collection.count() > 0:
                    # 모든 문서 ID 가져와서 삭제
                    all_ids = collection.get()['ids']
                    if all_ids:
                        collection.delete(ids=all_ids)
                        print(f"   - {len(all_ids)}개 기존 문서 삭제됨")
                else:
                    print("   - 삭제할 기존 문서 없음")
            except Exception as e:
                print(f"   - 기존 데이터 삭제 중 오류 (무시): {e}")
                # 오류가 발생해도 계속 진행
            
            # 문서 임베딩 및 저장
            print("⚡ 스키마 임베딩 진행 중...")
            self.vectorstore.add_documents(documents)
            
            # ChromaDB는 자동으로 persist되므로 별도 persist() 호출 불필요
            print("💾 벡터스토어 자동 저장됨")
            
            print(f"✅ 스키마 임베딩 완료!")
            print(f"   - 테이블 수: {len(schema_info)}개")
            print(f"   - 총 문서 수: {len(documents)}개")
            print(f"   - 저장 위치: {self.persist_directory}")
            
            return True
            
        except Exception as e:
            print(f"❌ 스키마 임베딩 실패: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_collection_info(self) -> Dict:
        """컬렉션 정보 조회"""
        if not self.vectorstore:
            return {}
        
        try:
            collection = self.vectorstore._collection
            count = collection.count()
            
            return {
                "collection_name": self.collection_name,
                "document_count": count,
                "persist_directory": self.persist_directory
            }
        except Exception as e:
            print(f"❌ 컬렉션 정보 조회 실패: {str(e)}")
            return {}

# 전역 임베더 인스턴스
schema_embedder = SchemaEmbedder()