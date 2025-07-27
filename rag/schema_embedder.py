"""
Schema Embedder - BigQuery 스키마 정보를 ChromaDB에 임베딩
"""

import os
import json
import hashlib
from datetime import datetime
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
        self.cache_metadata_file = os.path.join(persist_directory, "schema_cache.json")
        
    def initialize_vectorstore(self):
        """벡터스토어 초기화"""
        try:
            # persist_directory 생성
            os.makedirs(self.persist_directory, exist_ok=True)
            
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
    
    def generate_config_hash(self) -> str:
        """BigQuery 설정 정보의 해시값 생성"""
        from core.config import BIGQUERY_CONFIG
        
        # BigQuery 설정 정보를 해시에 포함
        config_data = {
            "keyfile_path": BIGQUERY_CONFIG.get("keyfile_path", ""),
            "default_dataset": BIGQUERY_CONFIG.get("default_dataset", ""),
            "target_tables": BIGQUERY_CONFIG.get("target_tables", [])
        }
        
        # 키파일이 존재하면 내용도 해시에 포함 (project_id 변경 감지)
        keyfile_path = config_data["keyfile_path"]
        if os.path.exists(keyfile_path):
            try:
                with open(keyfile_path, 'r') as f:
                    keyfile_data = json.load(f)
                    config_data["project_id"] = keyfile_data.get("project_id", "")
            except Exception:
                pass
        
        config_str = json.dumps(config_data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(config_str.encode('utf-8')).hexdigest()
    
    def generate_schema_hash(self, schema_info: Dict) -> str:
        """스키마 정보의 해시값 생성"""
        # 스키마 정보를 정렬된 JSON 문자열로 변환
        schema_str = json.dumps(schema_info, sort_keys=True, ensure_ascii=False)
        # SHA256 해시 생성
        return hashlib.sha256(schema_str.encode('utf-8')).hexdigest()
    
    def load_cache_metadata(self) -> Dict:
        """캐시 메타데이터 로드"""
        try:
            if os.path.exists(self.cache_metadata_file):
                with open(self.cache_metadata_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            print(f"⚠️ 캐시 메타데이터 로드 실패: {e}")
        return {}
    
    def save_cache_metadata(self, schema_hash: str, schema_info: Dict):
        """캐시 메타데이터와 스키마 정보 저장"""
        try:
            config_hash = self.generate_config_hash()
            metadata = {
                "schema_hash": schema_hash,
                "config_hash": config_hash,  # BigQuery 설정 해시 추가
                "last_updated": datetime.now().isoformat(),
                "table_count": len(schema_info),
                "table_names": list(schema_info.keys()),
                "schema_data": schema_info  # 스키마 정보도 함께 저장
            }
            
            with open(self.cache_metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
                
            print(f"💾 캐시 메타데이터 및 스키마 정보 저장 완료: {self.cache_metadata_file}")
            
        except Exception as e:
            print(f"⚠️ 캐시 메타데이터 저장 실패: {e}")
    
    def is_cache_valid(self, schema_info: Dict) -> bool:
        """캐시가 유효한지 확인"""
        current_hash = self.generate_schema_hash(schema_info)
        cached_metadata = self.load_cache_metadata()
        
        if not cached_metadata:
            print("📋 캐시 메타데이터가 없습니다.")
            return False
        
        cached_hash = cached_metadata.get("schema_hash")
        if not cached_hash:
            print("📋 저장된 스키마 해시가 없습니다.")
            return False
        
        # 벡터스토어에 실제 문서가 있는지도 확인
        try:
            if self.vectorstore:
                collection = self.vectorstore._collection
                doc_count = collection.count()
                if doc_count == 0:
                    print("📋 벡터스토어가 비어있습니다.")
                    return False
        except Exception as e:
            print(f"📋 벡터스토어 확인 중 오류: {e}")
            return False
        
        if current_hash == cached_hash:
            last_updated = cached_metadata.get("last_updated", "알 수 없음")
            table_count = cached_metadata.get("table_count", 0)
            print(f"✅ 캐시가 유효합니다!")
            print(f"   - 마지막 업데이트: {last_updated}")
            print(f"   - 테이블 수: {table_count}")
            print(f"   - 벡터 문서 수: {doc_count}")
            return True
        else:
            print("🔄 스키마가 변경되었습니다. 새로 임베딩합니다.")
            print(f"   - 이전 해시: {cached_hash[:12]}...")
            print(f"   - 현재 해시: {current_hash[:12]}...")
            return False
    
    def get_cached_schema_info(self) -> Dict:
        """캐시된 스키마 정보 반환"""
        cached_metadata = self.load_cache_metadata()
        if cached_metadata and "schema_data" in cached_metadata:
            return cached_metadata["schema_data"]
        return {}
    
    def has_valid_cache(self) -> bool:
        """캐시 유효성 확인 (BigQuery 설정 기반)"""
        try:
            cached_metadata = self.load_cache_metadata()
            if not cached_metadata or not cached_metadata.get("config_hash"):
                return False
            
            # BigQuery 설정이 변경되었는지 확인
            current_config_hash = self.generate_config_hash()
            cached_config_hash = cached_metadata.get("config_hash")
            
            if current_config_hash != cached_config_hash:
                print("🔄 BigQuery 설정이 변경되었습니다. 새로 조회합니다.")
                return False
            
            # 벡터스토어에 문서가 있는지 확인
            if self.vectorstore:
                collection = self.vectorstore._collection
                doc_count = collection.count()
                if doc_count > 0:
                    print("✅ BigQuery 설정이 동일하고 캐시가 유효합니다!")
                    return True
            
            return False
        except Exception as e:
            print(f"📋 캐시 확인 중 오류: {e}")
            return False
    
    def embed_schemas(self, schema_info: Dict) -> bool:
        """
        스키마 정보를 벡터스토어에 임베딩 (캐싱 지원)
        
        Args:
            schema_info: BigQuery 스키마 정보
            
        Returns:
            성공 여부
        """
        if not self.vectorstore:
            print("❌ 벡터스토어가 초기화되지 않았습니다.")
            return False
        
        try:
            # 캐시 유효성 검사
            if self.is_cache_valid(schema_info):
                print("🎯 기존 임베딩 캐시를 사용합니다.")
                return True
            
            # 캐시가 무효한 경우에만 새로 임베딩
            print("🔍 스키마 문서 생성 중...")
            documents = self.create_table_documents(schema_info)
            print(f"📝 생성된 문서 수: {len(documents)}개")
            
            # 스키마가 변경된 경우에만 기존 데이터 교체
            print("🔄 기존 임베딩 데이터 교체 중...")
            try:
                collection = self.vectorstore._collection
                existing_count = collection.count()
                
                if existing_count > 0:
                    # 기존 문서 삭제
                    all_ids = collection.get()['ids']
                    if all_ids:
                        collection.delete(ids=all_ids)
                        print(f"   - {len(all_ids)}개 기존 문서 삭제됨")
                else:
                    print("   - 기존 문서 없음 (첫 임베딩)")
                    
            except Exception as e:
                print(f"   - 기존 데이터 삭제 중 오류 (무시): {e}")
            
            # 새 문서 임베딩 및 저장
            print("⚡ 새 스키마 임베딩 진행 중...")
            self.vectorstore.add_documents(documents)
            
            # ChromaDB는 자동으로 persist되므로 별도 persist() 호출 불필요
            print("💾 벡터스토어 자동 저장됨")
            
            # 캐시 메타데이터 저장
            schema_hash = self.generate_schema_hash(schema_info)
            self.save_cache_metadata(schema_hash, schema_info)
            
            print(f"✅ 스키마 임베딩 완료!")
            print(f"   - 테이블 수: {len(schema_info)}개")
            print(f"   - 총 문서 수: {len(documents)}개")
            print(f"   - 저장 위치: {self.persist_directory}")
            print(f"   - 스키마 해시: {schema_hash[:12]}...")
            
            return True
            
        except Exception as e:
            print(f"❌ 스키마 임베딩 실패: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_collection_info(self) -> Dict:
        """컬렉션 정보 조회 (캐시 정보 포함)"""
        if not self.vectorstore:
            return {}
        
        try:
            collection = self.vectorstore._collection
            count = collection.count()
            
            info = {
                "collection_name": self.collection_name,
                "document_count": count,
                "persist_directory": self.persist_directory
            }
            
            # 캐시 메타데이터 추가
            cache_metadata = self.load_cache_metadata()
            if cache_metadata:
                info.update({
                    "cache_last_updated": cache_metadata.get("last_updated"),
                    "cache_table_count": cache_metadata.get("table_count"),
                    "cache_schema_hash": cache_metadata.get("schema_hash", "")[:12] + "..."
                })
            
            return info
            
        except Exception as e:
            print(f"❌ 컬렉션 정보 조회 실패: {str(e)}")
            return {}
    
    def clear_cache(self):
        """캐시 완전 삭제"""
        try:
            print("🗑️ 캐시 삭제 중...")
            
            # 캐시 메타데이터 파일 삭제
            if os.path.exists(self.cache_metadata_file):
                os.remove(self.cache_metadata_file)
                print("   - 캐시 메타데이터 삭제됨")
            
            # ChromaDB 벡터스토어 삭제
            if self.vectorstore:
                try:
                    collection = self.vectorstore._collection
                    if collection.count() > 0:
                        all_ids = collection.get()['ids']
                        if all_ids:
                            collection.delete(ids=all_ids)
                            print(f"   - {len(all_ids)}개 벡터 문서 삭제됨")
                except Exception as e:
                    print(f"   - 벡터스토어 삭제 중 오류: {e}")
            
            print("✅ 캐시 삭제 완료")
            
        except Exception as e:
            print(f"❌ 캐시 삭제 실패: {str(e)}")
    
    def initialize_with_cache(self, bq_client) -> Dict:
        """
        캐시를 활용한 스키마 초기화
        캐시가 유효하면 BigQuery API 호출 없이 캐시된 데이터 사용
        """
        print("🔍 캐시 기반 스키마 초기화 중...")
        
        # 벡터스토어 초기화
        if not self.initialize_vectorstore():
            return {}
        
        # 캐시 유효성 확인
        if self.has_valid_cache():
            print("✅ 유효한 캐시 발견!")
            cached_schema = self.get_cached_schema_info()
            if cached_schema:
                print(f"🎯 캐시된 스키마 사용: {len(cached_schema)}개 테이블 (BigQuery API 호출 없음)")
                cached_metadata = self.load_cache_metadata()
                if cached_metadata:
                    last_updated = cached_metadata.get("last_updated", "").split('T')[0]
                    print(f"📅 마지막 업데이트: {last_updated}")
                # BigQuery 클라이언트는 사용하지 않고 캐시된 데이터만 반환
                return cached_schema
        
        # 캐시가 없거나 무효한 경우 BigQuery에서 새로 조회
        print("🔗 BigQuery에서 스키마 정보 조회 중...")
        if not bq_client.connect():
            return {}
        
        schema_info = bq_client.initialize_schema()
        if not schema_info:
            return {}
        
        # 새로 조회한 스키마 임베딩
        if not self.embed_schemas(schema_info):
            return {}
        
        return schema_info

# 전역 임베더 인스턴스
schema_embedder = SchemaEmbedder()