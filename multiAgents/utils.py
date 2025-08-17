def print_graph(graph):
    """환경에 따라 그래프를 이미지 또는 텍스트로 출력합니다."""
    try:
        # Jupyter/IPython 환경인지 확인
        from IPython.display import Image, display
        png_data = graph.get_graph().draw_mermaid_png()
        print("--- Workflow Diagram ---")
        display(Image(png_data))
        print("----------------------")
    except ImportError:
        # 일반 터미널 환경
        mermaid_md = graph.get_graph().draw_mermaid()
        print("--- Workflow Diagram (Mermaid Markdown) ---")
        print("아래 텍스트를 복사하여 Mermaid 지원 뷰어(예: https://mermaid.live)에 붙여넣으세요.")
        print("```mermaid")
        print(mermaid_md)
        print("```")
        print("-------------------------------------------")