# SQL Generator - 명령어 단축키

.PHONY: test test-bq test-rag test-sql test-analyzer test-explorer run workflow install clean-cache help

# BigQuery 연결 테스트
test-bq:
	python3 tests/test_bigquery.py

# RAG 시스템 테스트
test-rag:
	python3 tests/test_rag.py

# SQL 실행 기능 테스트
test-sql:
	python3 tests/test_sql_execution.py

# SQL 분석기 테스트
test-analyzer:
	python3 tests/test_sql_analyzer.py

# SQL 탐색기 테스트
test-explorer:
	python3 tests/test_sql_explorer.py

# 전체 시스템 테스트 (BigQuery + RAG + SQL 실행 + 분석기 + 탐색기)
test: test-bq test-rag test-sql test-analyzer test-explorer

# 메인 프로그램 실행 
run:
	python3 main.py

# 워크플로우 직접 실행
workflow:
	cd . && python3 -m workflow.workflow

# 패키지 설치
install:
	pip3 install -r requirements.txt

# RAG 캐시 삭제
clean-cache:
	@echo "🗑️ RAG 캐시 삭제 중..."
	@python3 -c "from rag.schema_embedder import schema_embedder; schema_embedder.clear_cache()"
	@echo "✅ 캐시 삭제 완료"

# 도움말
help:
	@echo "사용 가능한 명령어:"
	@echo "  make test        - 전체 시스템 테스트 (BigQuery + RAG + SQL 실행 + 분석기 + 탐색기)"
	@echo "  make test-bq     - BigQuery 연결 테스트"
	@echo "  make test-rag    - RAG 시스템 테스트"
	@echo "  make test-sql    - SQL 실행 기능 테스트"
	@echo "  make test-analyzer - SQL 분석기 테스트"
	@echo "  make test-explorer - SQL 탐색기 테스트"
	@echo "  make run         - 메인 프로그램 실행"
	@echo "  make workflow    - 워크플로우 직접 실행"
	@echo "  make install     - 패키지 설치"
	@echo "  make clean-cache - RAG 캐시 삭제"
	@echo "  make help        - 이 도움말 표시"