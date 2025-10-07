from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, ListFlowable, ListItem
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib import colors
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Polygon
from reportlab.graphics import renderPDF
import os


def build_doc(output_path: str):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="TitleCenter", parent=styles["Title"], alignment=TA_CENTER))
    styles.add(ParagraphStyle(name="SectionHeader", parent=styles["Heading2"], spaceAfter=6, textColor=colors.HexColor("#2F4F4F")))
    styles.add(ParagraphStyle(name="SubHeader", parent=styles["Heading3"], spaceAfter=4, textColor=colors.HexColor("#3A6EA5")))
    styles.add(ParagraphStyle(name="Body", parent=styles["BodyText"], leading=14))

    doc = SimpleDocTemplate(output_path, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=36, bottomMargin=36)
    story = []

    # Título
    story.append(Paragraph("Roteiro da Apresentação do Projeto", styles["TitleCenter"]))
    story.append(Spacer(1, 12))
    story.append(Paragraph("Equipe e tópicos de fala por responsabilidade e entregáveis", styles["Body"]))
    story.append(Spacer(1, 18))

    # Visão geral técnica
    story.append(Paragraph("Visão Geral Técnica", styles["SectionHeader"]))
    overview_items = [
        "Arquitetura com Spark standalone orquestrado por Docker Compose",
        "HDFS configurado com fs.defaultFS em hdfs://namenode:8020",
        "UIs: Spark Master em 8080, Jupyter (spark-client) em 8888, NameNode em 9870, DataNode em 9864",
        "Pipeline: Coleta → Curadoria → Janelas de evento → Notebooks/Visualizações",
    ]
    story.append(ListFlowable([ListItem(Paragraph(i, styles["Body"])) for i in overview_items], bulletType="bullet", start="•"))
    story.append(Spacer(1, 12))

    def section(nome, responsabilidades, entregaveis, roteiro_fala):
        story.append(Paragraph(nome, styles["SectionHeader"]))
        story.append(Paragraph("Responsabilidades", styles["SubHeader"]))
        story.append(ListFlowable([ListItem(Paragraph(r, styles["Body"])) for r in responsabilidades], bulletType="bullet", start="•"))
        story.append(Spacer(1, 6))
        story.append(Paragraph("Entregáveis", styles["SubHeader"]))
        story.append(ListFlowable([ListItem(Paragraph(e, styles["Body"])) for e in entregaveis], bulletType="bullet", start="•"))
        story.append(Spacer(1, 6))
        story.append(Paragraph("Roteiro de Fala", styles["SubHeader"]))
        for p in roteiro_fala:
            story.append(Paragraph(p, styles["Body"]))
            story.append(Spacer(1, 4))
        story.append(Spacer(1, 12))

    # Ana Luiza Pazze - Arquitetura e Infraestrutura
    section(
        "Ana Luiza Pazze — Arquitetura e Infraestrutura",
        [
            "Configuração do ambiente Apache Spark",
            "Implementação da arquitetura de dados distribuída",
            "Configuração do HDFS e sistemas de armazenamento",
            "Otimização de performance do cluster Spark",
            "Implementação de pipelines ETL",
        ],
        [
            "Documentação da arquitetura técnica",
            "Scripts de configuração do ambiente",
            "Pipeline de ETL funcional",
            "Relatório de performance e otimizações",
        ],
        [
            "Apresentar a topologia do cluster (master, workers) e como o Docker Compose orquestra os serviços.",
            "Explicar o HDFS: fs.defaultFS (hdfs://namenode:8020), partições e políticas de replicação.",
            "Mostrar como os notebooks e jobs se conectam ao cluster e ao HDFS.",
            "Comentar otimizações aplicadas (memória do worker, paralelismo, particionamento por símbolo).",
        ],
    )

    # Felipe Martins - API e Coleta de Dados
    section(
        "Felipe Martins — API e Coleta de Dados",
        [
            "Implementação da integração com Yahoo Finance API",
            "Desenvolvimento de coletores de dados de notícias",
            "Implementação de web scraping para dados complementares",
            "Tratamento de rate limits e otimização de requisições",
            "Validação e limpeza de dados coletados",
        ],
        [
            "Módulos de coleta de dados funcionais",
            "Documentação das APIs utilizadas",
            "Scripts de validação de dados",
            "Relatório de qualidade dos dados coletados",
        ],
        [
            "Demonstrar o coletor Yahoo Finance (símbolos, colunas, limpeza inicial e deduplicação).",
            "Descrever estratégias para rate limits (pausas, retries) e fontes complementares.",
            "Explicar validações: datas válidas, presença de preços e volumes, símbolos com categoria.",
            "Indicar como os dados brutos são enviados ao HDFS e integrados ao pipeline.",
        ],
    )

    # Pedro Silva - Análise de Dados
    section(
        "Pedro Silva — Análise de Dados",
        [
            "Análise exploratória dos dados financeiros",
            "Implementação de análises estatísticas",
            "Desenvolvimento de métricas de impacto",
            "Análise de correlações e causalidade",
            "Validação estatística dos resultados",
        ],
        [
            "Relatórios de análise exploratória",
            "Implementação de testes estatísticos",
            "Métricas de impacto definidas e calculadas",
            "Relatório de correlações identificadas",
        ],
        [
            "Mostrar EDA: distribuição por categorias, evolução temporal e cobertura dos símbolos.",
            "Descrever métricas de impacto com base em janelas de evento (pré/durante/pós).",
            "Apresentar correlações entre categorias e discutir hipóteses de causalidade.",
            "Citar testes estatísticos aplicados e critérios de significância.",
        ],
    )

    # Anny Caroline Sousa - Machine Learning
    section(
        "Anny Caroline Sousa — Machine Learning",
        [
            "Desenvolvimento de modelos preditivos",
            "Implementação de algoritmos de classificação",
            "Análise de sentimento de notícias",
            "Otimização de hiperparâmetros",
            "Validação e avaliação de modelos",
        ],
        [
            "Modelos de machine learning treinados",
            "Relatório de performance dos modelos",
            "Sistema de análise de sentimento",
            "Documentação dos algoritmos implementados",
        ],
        [
            "Apresentar abordagem de features (preços, índices normalizados, sinais de notícias).",
            "Descrever modelos e tarefas (regressão, classificação, previsão de impacto).",
            "Explicar avaliação: métricas, validação cruzada e ajustes de hiperparâmetros.",
            "Mostrar como os resultados se integram ao dashboard e às janelas de evento.",
        ],
    )

    # Ricardo Areas - Visualização e Dashboard
    section(
        "Ricardo Areas — Visualização e Dashboard",
        [
            "Desenvolvimento de visualizações interativas",
            "Criação de dashboards executivos",
            "Implementação de relatórios automatizados",
            "Design de interface de usuário",
            "Otimização de performance das visualizações",
        ],
        [
            "Dashboard interativo funcional",
            "Biblioteca de visualizações reutilizáveis",
            "Relatórios automatizados",
            "Documentação de uso das visualizações",
        ],
        [
            "Demonstrar gráfico interativo com filtros de evento e categoria (Plotly/ipywidgets).",
            "Explicar layout e usabilidade do dashboard executivo e KPIs exibidos.",
            "Comentar automações de relatórios e parâmetros ajustáveis.",
            "Apontar otimizações de performance para volumes maiores de dados.",
        ],
    )

    # Fabio Silva - Gestão de Projeto
    section(
        "Fabio Silva — Gestão de Projeto",
        [
            "Coordenação geral do projeto",
            "Gestão de cronograma e entregas",
            "Documentação técnica e acadêmica",
            "Preparação de apresentações",
            "Controle de qualidade e integração",
        ],
        [
            "Documentação completa do projeto",
            "Relatório final acadêmico",
            "Apresentações executivas",
            "Plano de projeto e cronograma",
        ],
        [
            "Contextualizar objetivos, escopo e marcos do cronograma.",
            "Apresentar integrações entre times e gestão de riscos.",
            "Destacar entregáveis, qualidade e próximos passos.",
        ],
    )

    # Encerramento
    story.append(PageBreak())
    story.append(Paragraph("Referências úteis", styles["SectionHeader"]))
    refs = [
        "Docker Desktop (imagens de containers e volumes)",
        "UIs: http://localhost:8080 (Spark Master), http://localhost:9870 (NameNode), http://localhost:9864 (DataNode), http://localhost:8888 (Jupyter)",
        "HDFS base: /datasets/yahoo_finance/curated",
        "Notebooks: notebooks/event_analysis_hdfs.ipynb",
    ]
    story.append(ListFlowable([ListItem(Paragraph(r, styles["Body"])) for r in refs], bulletType="bullet", start="•"))
    story.append(Spacer(1, 12))

    # Desenho de Arquitetura
    story.append(PageBreak())
    story.append(Paragraph("Desenho de Arquitetura", styles["SectionHeader"]))
    story.append(Paragraph("Relação entre Docker Compose, Spark, HDFS e Visualização.", styles["Body"]))
    # Legenda posicionada ACIMA do desenho para evitar sobreposição
    story.append(Paragraph("Legenda do Diagrama", styles["SubHeader"]))
    legend_items = [
        "Orquestração: Docker Compose (rede spark-hadoop-net)",
        "UIs: 8080(Spark), 8888(Jupyter), 9870(NameNode), 9864(DataNode)",
        "Dados: raw → curated/prices → curated/event_windows",
    ]
    story.append(ListFlowable([ListItem(Paragraph(i, styles["Body"])) for i in legend_items], bulletType="bullet", start="•"))
    story.append(Spacer(1, 8))

    # tamanho disponível na página A4 considerando margens
    avail_w = A4[0] - doc.leftMargin - doc.rightMargin
    avail_h = A4[1] - doc.topMargin - doc.bottomMargin

    def make_architecture_drawing(avw, avh):
        base_w, base_h = 800, 420
        dw = Drawing(base_w, base_h)

        # Título do desenho
        dw.add(String(300, 395, "Arquitetura Geral do Projeto", fontSize=14))

        # Boxes principais
        def box(x, y, w, h, label):
            dw.add(Rect(x, y, w, h, strokeColor=colors.HexColor('#2F4F4F'), fillColor=colors.HexColor('#F5F7FA')))
            dw.add(String(x + 6, y + h - 18, label, fontSize=10))

        # Spark cluster
        box(40, 310, 200, 60, "Spark Master (UI 8080)")
        box(260, 310, 200, 60, "Spark Worker-1 (UI 8081)")
        box(480, 310, 260, 60, "Spark Client — Jupyter (UI 8888)")

        # HDFS
        box(80, 210, 260, 60, "HDFS NameNode (UI 9870, RPC 8020)")
        box(360, 210, 260, 60, "HDFS DataNode (UI 9864)")

        # Storage paths
        box(120, 110, 520, 70, "/datasets/yahoo_finance/curated\nprices, event_windows (Parquet)")

        # Visualização
        box(520, 30, 200, 60, "Notebooks/Visualização (Plotly + ipywidgets)")

        # Conexões (linhas e setas)
        def arrow(x1, y1, x2, y2):
            dw.add(Line(x1, y1, x2, y2, strokeColor=colors.HexColor('#3A6EA5')))
            # simples cabeça de seta
            hx = x2
            hy = y2
            dx = -6 if x2 >= x1 else 6
            dy = 6 if y2 >= y1 else -6
            dw.add(Polygon(points=[hx, hy, hx+dx, hy+dy, hx+dy, hy-dx], strokeColor=colors.HexColor('#3A6EA5'), fillColor=colors.HexColor('#3A6EA5')))

        # Client -> Master
        arrow(610, 310, 140, 270)
        dw.add(String(430, 280, "Job submit — spark://spark-master:7077", fontSize=9))

        # Master -> Worker
        arrow(240, 340, 260, 340)
        dw.add(String(240, 350, "Distribuição de tarefas", fontSize=9))

        # Spark -> NameNode (fs.defaultFS)
        arrow(140, 310, 210, 270)
        dw.add(String(110, 290, "fs.defaultFS = hdfs://namenode:8020", fontSize=9))

        # NameNode <-> DataNode
        arrow(340, 240, 360, 240)
        dw.add(String(330, 250, "Metadados ↔ Blocos", fontSize=9))

        # NameNode -> Storage paths
        arrow(210, 210, 210, 180)

        # Storage -> Visualização
        arrow(380, 110, 540, 90)
        dw.add(String(420, 100, "Leitura Parquet para gráficos", fontSize=9))

        # Legendas gerais removidas (exibidas acima do desenho)

        # escala para caber na página (com pequena folga)
        scale = min((avw - 10) / base_w, (avh - 150) / base_h)
        if scale < 1.0:
            dw.scale(scale, scale)
        return dw

    story.append(make_architecture_drawing(avail_w, avail_h))
    story.append(Spacer(1, 12))

    # Explicação detalhada
    story.append(Paragraph("Explicação Detalhada do Desenvolvimento e Configurações", styles["SectionHeader"]))
    expl = [
        "Orquestração: serviços Spark (master, worker, client) e HDFS (namenode, datanode) sob Docker Compose em rede comum.",
        "Configurações: spark-defaults define fs.defaultFS=hdfs://namenode:8020; jobs/notebooks usam esse endpoint para IO.",
        "Coleta: módulo Yahoo Finance produz CSVs brutos, enviados ao HDFS em /datasets/yahoo_finance/raw.",
        "Curadoria: job Spark lê CSVs, normaliza colunas (datas, AdjClose), enriquece com categorias e escreve Parquet em curated/prices.",
        "Janelas de Evento: job Spark recorta pre/during/post por âncora, particiona por categoria/símbolo e escreve em curated/event_windows.",
        "Análise/Visualização: notebook lê event_windows do HDFS, normaliza índices por âncora, agrega por categoria/data e plota gráficos interativos.",
        "Fluxo de dados: Spark lê/escreve via NameNode que referencia blocos no DataNode; visualização consome Parquet diretamente do HDFS.",
        "Observabilidade: UIs expostas — Spark (8080), Jupyter (8888), NameNode (9870), DataNode (9864) para acompanhar jobs e estado de armazenamento.",
    ]
    story.append(ListFlowable([ListItem(Paragraph(e, styles["Body"])) for e in expl], bulletType="bullet", start="•"))

    # Base 100
    story.append(Spacer(1, 12))
    story.append(Paragraph("Normalização Base 100 (Índice)", styles["SectionHeader"]))
    base100_intro = [
        "Definição: índice = (preço atual / preço de referência) × 100.",
        "Preço de referência (baseline): valor por categoria no dia da âncora do evento; se indisponível, usa-se o primeiro dia disponível por categoria.",
        "Interpretação: 100 representa o valor na âncora; 110 = +10% em relação à âncora; 95 = −5%.",
        "Vantagens: torna comparáveis categorias com escalas diferentes; facilita leitura de impactos relativos.",
        "Limitações: perde magnitude absoluta; sensível à escolha da âncora e à cobertura de dados no dia da âncora.",
        "Implementação: normalize_by_anchor_spark cria a coluna 'index'; aggregate_for_plot agrega por 'category' e 'date' para visualização.",
    ]
    story.append(ListFlowable([ListItem(Paragraph(t, styles["Body"])) for t in base100_intro], bulletType="bullet", start="•"))
    story.append(Spacer(1, 6))
    story.append(Paragraph("Exemplo: se a categoria A tem preço de referência 50 na âncora e preço 55 em um dia posterior, então índice = (55/50)×100 = 110 (crescimento de 10%).", styles["Body"]))

    doc.build(story)


if __name__ == "__main__":
    out = os.path.join("docs", "Apresentacao_Projeto_Roteiro.pdf")
    build_doc(out)
    print(f"PDF gerado: {out}")