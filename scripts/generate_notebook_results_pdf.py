import os
import base64
import nbformat
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak, ListFlowable, ListItem
from reportlab.graphics.shapes import Drawing, String, Polygon
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib import colors


def extract_images_from_notebook(nb_path: str, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    with open(nb_path, 'r', encoding='utf-8') as f:
        nb = nbformat.read(f, as_version=4)

    images = []  # list of dicts: {path, caption}
    current_event_caption = None

    for cell in nb.get('cells', []):
        # capture event caption from stream outputs
        for out in cell.get('outputs', []):
            if out.get('output_type') == 'stream' and isinstance(out.get('text'), str):
                txt = out['text']
                if 'Evento:' in txt:
                    # use the last line containing "Evento:" as current caption
                    lines = [l.strip() for l in txt.splitlines() if 'Evento:' in l]
                    if lines:
                        current_event_caption = lines[-1]

        # extract image/png from display_data or execute_result
        for out in cell.get('outputs', []):
            if out.get('output_type') in ('display_data', 'execute_result'):
                data = out.get('data', {})
                img_b64 = data.get('image/png')
                if img_b64:
                    idx = len(images) + 1
                    img_path = os.path.join(out_dir, f'notebook_img_{idx:03d}.png')
                    with open(img_path, 'wb') as imgf:
                        imgf.write(base64.b64decode(img_b64))
                    images.append({'path': img_path, 'caption': current_event_caption})

    return images


def build_pdf(images, output_path: str):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="TitleCenter", parent=styles["Title"], alignment=TA_CENTER))
    styles.add(ParagraphStyle(name="SectionHeader", parent=styles["Heading2"], spaceAfter=6, textColor=colors.HexColor("#2F4F4F")))
    styles.add(ParagraphStyle(name="SubHeader", parent=styles["Heading3"], spaceAfter=4, textColor=colors.HexColor("#3A6EA5")))
    styles.add(ParagraphStyle(name="Body", parent=styles["BodyText"], leading=14))

    doc = SimpleDocTemplate(output_path, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=36, bottomMargin=36)
    story = []

    def build_spark_badge():
        # Selo visual do Spark próximo ao título do evento
        d = Drawing(140, 28)
        orange = colors.HexColor('#E25A1C')
        star = Polygon(points=[10,14, 14,24, 8,18, 2,24, 6,14, 2,4, 8,10, 14,4],
                       fillColor=orange, strokeColor=orange)
        d.add(star)
        d.add(String(30, 8, "Apache Spark", fontSize=12, fillColor=orange))
        return d

    # Título
    story.append(Paragraph("Resultados do Notebook — Gráficos e Explicações", styles["TitleCenter"]))
    story.append(Spacer(1, 12))
    story.append(Paragraph("Origem: notebooks/event_analysis_hdfs.ipynb", styles["Body"]))
    story.append(Spacer(1, 12))

    # Explicação geral dos gráficos
    story.append(Paragraph("Contexto dos Gráficos", styles["SectionHeader"]))
    ctx_items = [
        "Cada figura representa o índice Base 100 por categoria ao longo da janela do evento.",
        "As janelas são unidas (pré/durante/pós) e agregadas por 'category' e 'date'.",
        "A linha de base (100) corresponde ao valor na data da âncora do evento.",
        "Os dados são lidos diretamente do HDFS e normalizados no notebook.",
    ]
    story.append(ListFlowable([ListItem(Paragraph(i, styles["Body"])) for i in ctx_items], bulletType="bullet", start="•"))
    story.append(Spacer(1, 12))

    # Inserir imagens agrupadas por título de evento, iniciando cada evento em nova página
    grouped = {}
    for item in images:
        cap = item.get('caption') or "Evento (sem título)"
        grouped.setdefault(cap, []).append(item)

    first_event = True
    for cap, items in grouped.items():
        if not first_event:
            story.append(PageBreak())
        first_event = False
        # Título do evento no início da página + badge do Spark para ênfase
        story.append(Paragraph(cap, styles["SectionHeader"]))
        story.append(build_spark_badge())
        story.append(Spacer(1, 8))
        # Imagens do evento
        for idx, it in enumerate(items, start=1):
            img = Image(it['path'])
            avail_w = A4[0] - doc.leftMargin - doc.rightMargin
            iw, ih = img.wrap(0, 0)
            scale = min(avail_w / iw, 1.0)
            img.drawWidth = iw * scale
            img.drawHeight = ih * scale
            story.append(img)
            story.append(Spacer(1, 12))

    if not images:
        story.append(Paragraph("Nenhuma imagem encontrada no notebook. Verifique se os gráficos foram executados e renderizados (image/png).", styles["Body"]))

    doc.build(story)


if __name__ == "__main__":
    nb_path = os.path.join("notebooks", "event_analysis_hdfs.ipynb")
    out_dir = os.path.join("docs", "notebook_images")
    pdf_out = os.path.join("docs", "Notebook_Resultados.pdf")

    imgs = extract_images_from_notebook(nb_path, out_dir)
    build_pdf(imgs, pdf_out)
    print(f"PDF gerado: {pdf_out} | Imagens extraídas: {len(imgs)} em {out_dir}")