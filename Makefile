# SQL Generator - 명령어 단축키

.PHONY: test test-bq test-rag run workflow install help

# BigQuery 연결 테스트
test-bq:
	python3 tests/test_bigquery.py

# RAG 시스템 테스트
test-rag:
	python3 tests/test_rag.py

# 전체 시스템 테스트 (BigQuery + RAG)
test: test-bq test-rag

# 메인 프로그램 실행 
run:
	python3 main.py

# 워크플로우 직접 실행
workflow:
	cd . && python3 -m workflow.workflow

# 패키지 설치
install:
	pip3 install -r requirements.txt

# 도움말
help:
	@echo "사용 가능한 명령어:"
	@echo "  make test      - 전체 시스템 테스트 (BigQuery + RAG)"
	@echo "  make test-bq   - BigQuery 연결 테스트"
	@echo "  make test-rag  - RAG 시스템 테스트"
	@echo "  make run       - 메인 프로그램 실행"
	@echo "  make workflow  - 워크플로우 직접 실행"
	@echo "  make install   - 패키지 설치"
	@echo "  make help      - 이 도움말 표시"