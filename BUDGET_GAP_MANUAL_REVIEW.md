# Budget Gap Explorer — Manual Review Instructions

> **Propósito:** Completar la columna "Analysis" de la tabla que aparece en la
> sección **Budget Gap Explorer** de nuestra app Streamlit, revisando manualmente
> cada documento que corresponde a un año sin datos en la base.
>
> Este archivo es tu guía de trabajo. Cuando termines, tendrás que llenar
> una tabla con una fila por año faltante.

---

## Contexto del proyecto

Estamos construyendo una base de datos de gasto en I+D por país y año a partir
de **Finance Bills** (presupuestos nacionales publicados como documentos oficiales).

El pipeline:
1. Lee cada archivo fuente (PDF o DOCX) de `Data/input/finance_bills/<Country>/`
2. Usa un LLM (GPT-4o) para identificar líneas presupuestarias de I+D
3. Extrae: agencia, monto, año, categoría
4. Escribe los resultados en `Data/output/budget/results.csv`

Un "año faltante" (gap) es un año dentro del rango histórico de un país donde
**no tenemos ninguna fila** en `results.csv`. Puede ser porque:
- El pipeline nunca procesó ese año (no hay documento)
- El pipeline falló (error en run_log.jsonl)
- El documento fue procesado pero el LLM no encontró líneas de I+D
- El documento no contiene datos de I+D (presupuesto de otro sector)

---

## Tu tarea

Para cada gap listado al final de este archivo:

1. **Localiza el documento** en `Data/input/finance_bills/<Country>/`
   - Si no hay documento, anota: "No document found for this year"

2. **Lee el documento** (PDF/DOCX) — usa `fitz` (PyMuPDF) o `python-docx`
   para extraer el texto de las primeras páginas y buscar palabras clave de I+D

3. **Determina la causa del gap** — elige una de estas:
   - `no_document` — no existe archivo de ese año
   - `pipeline_error_format` — el archivo existe pero es `.doc` (binario Word 97) que python-docx no lee
   - `pipeline_error_rate_limit` — el pipeline falló por límite de API (re-correr)
   - `pipeline_error_code` — bug en nuestro código (BudgetRow bug ya corregido, re-correr)
   - `no_rd_content` — el documento existe y fue procesado, pero no contiene líneas de I+D
   - `document_unclear` — el documento existe pero es ilegible/dañado/vacío
   - `needs_rerun` — procesado con error, necesita re-correr el pipeline

4. **Escribe el análisis** en la columna "Analysis" de la tabla de resultados (ver formato abajo)

---

## Cómo identificar los gaps

Corre este script desde la raíz del proyecto para obtener todos los gaps actuales
por país:

```bash
python3 - <<'EOF'
import pandas as pd, json
from pathlib import Path

results = pd.read_csv("Data/output/budget/results.csv")
run_log_path = Path("Data/output/budget/run_log.jsonl")

log_rows = []
if run_log_path.exists():
    with open(run_log_path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    log_rows.append(json.loads(line))
                except Exception:
                    pass
log_df = pd.DataFrame(log_rows) if log_rows else pd.DataFrame()

for country, grp in results.groupby("country"):
    years_with_data = set(grp["year"].dropna().astype(int).unique())
    if not years_with_data:
        continue
    full_range = set(range(min(years_with_data), max(years_with_data) + 1))
    gaps = sorted(full_range - years_with_data)
    if not gaps:
        continue
    print(f"\n=== {country} — gaps: {gaps}")
    for yr in gaps:
        if not log_df.empty and "country" in log_df.columns:
            yr_log = log_df[(log_df["country"].str.lower() == country.lower()) & (log_df["year"] == yr)]
            docs = ", ".join(yr_log["source_file"].dropna().unique()) if not yr_log.empty else "—"
            statuses = yr_log["status"].unique().tolist() if not yr_log.empty else []
            errors = yr_log["error"].dropna().unique().tolist() if "error" in yr_log.columns else []
        else:
            docs = "—"
            statuses = []
            errors = []
        print(f"  {yr}: docs={docs} | statuses={statuses} | errors={errors[:1]}")
EOF
```

---

## Cómo inspeccionar un documento

```python
# Para PDFs:
import fitz
doc = fitz.open("Data/input/finance_bills/Country/filename.pdf")
for page in doc[:5]:
    print(page.get_text())

# Para DOCX:
import docx
doc = docx.Document("Data/input/finance_bills/Country/filename.docx")
for para in doc.paragraphs[:50]:
    print(para.text)

# Para saber si un año tiene archivos en disco:
import os
files = [f for f in os.listdir("Data/input/finance_bills/Country/") if "2001" in f]
print(files)
```

**Palabras clave de I+D que buscar** (en el idioma del país):
- Inglés: research, R&D, science, innovation, CSIRO, ARC, NSERC
- Español: investigación, I+D, COLCIENCIAS, CONICET, ciencia
- Alemán: Forschung, Wissenschaft, Bundesministerium
- Francés: recherche, science, CNRS, ANR
- Japonés: 研究, 科学技術

---

## Formato de output

Al terminar, llena esta tabla para **cada gap** que investigues.
Copia el bloque de resultados al final de este archivo bajo `## Resultados`.

```
### [País] [Año]
- **Documento encontrado:** Sí / No — `nombre_del_archivo.pdf`
- **Causa del gap:** [no_document | pipeline_error_format | pipeline_error_rate_limit | pipeline_error_code | no_rd_content | document_unclear | needs_rerun]
- **¿Contiene datos de I+D?** Sí / No / No está claro
- **Acción recomendada:** [ninguna | re-correr pipeline | convertir .doc a .docx | buscar fuente alternativa | marcar como sin datos]
- **Notas:** texto libre — qué encontraste, por qué ese año tiene gap, qué líneas de I+D hay o no hay
```

---

## Archivos de referencia útiles

- `Data/output/budget/run_log.jsonl` — log de cada archivo procesado (status, error)
- `Data/output/budget/results.csv` — todas las filas extraídas hasta ahora
- `Data/input/finance_bills/` — documentos fuente por país
- `PIPELINE_FAILURES_REVIEW.md` — análisis de errores técnicos conocidos (Australia .doc, BudgetRow bug, rate limits)

---

## Resultados

> Llena esta sección con un bloque por gap investigado (ver formato arriba).

