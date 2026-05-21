# Pipeline Failures — Manual Review Instructions

> **Propósito de este archivo:** Guía para que otro agente/chat revise manualmente
> los documentos que fallaron en el pipeline de extracción de Finance Bills y
> diagnostique por qué cada año quedó vacío en la base de datos.

---

## Contexto del proyecto

Estamos construyendo una base de datos de gasto en I+D por país y año a partir de
**Finance Bills** (presupuestos nacionales publicados como documentos oficiales).
El pipeline:
1. Lee cada archivo fuente (PDF o DOCX)
2. Usa un LLM (GPT-4o) para identificar líneas presupuestarias de I+D
3. Extrae: agencia, monto, año, categoría
4. Escribe los resultados en `Data/output/budget/<Country>/`

Cuando un archivo falla, se escribe `status: error` en `Data/output/budget/run_log.jsonl`
y ese año queda vacío en la base.

**Tu tarea:** Por cada grupo de fallos a continuación, determinar:
1. ¿El documento realmente existe y es legible?
2. ¿Contiene datos de I+D que deberíamos tener?
3. ¿El fallo es un problema nuestro (código/formato) o del documento?
4. ¿Qué acción recomendamos?

---

## Tipo A — Formato `.doc` no soportado
**Error:** `Package not found` (python-docx solo lee `.docx`, no `.doc` binario de Word 97-2003)
**Fix técnico disponible:** Convertir con `soffice --headless --convert-to docx`

### Australia — 1999 a 2013 (15 años)

Los documentos son Appropriation Acts del gobierno australiano en formato `.doc`.
Los archivos físicos SÍ existen en `Data/input/finance_bills/Australia/`.

| Año  | Archivos que fallaron |
|------|-----------------------|
| 1999 | `1999 No1 SUPPL C2004A00545.doc`, `1999 No2 SUPPL C2004A00546.doc` |
| 2000 | `2000 DEPT C2004A00439.doc`, `2000 No1 C2008C00464.doc`, `2000 No2 C2011C00102.doc`, +2 |
| 2001 | `2001 DEPT C2004A00678.doc`, `2001 DEPT C2004A00786.doc`, `2001 No1 C2008C00481.doc`, +5 |
| 2002 | `2002 DEPT 1 C2004A00938VOL01.doc`, `2002 DEPT 2 C2004A00938VOL02.doc`, +6 |
| 2003 | `2003 DEPT C2004A00996.doc`, `2003 No1 C2008C00466.doc`, +3 |
| 2004 | `2004 DEPT C2004A01141.doc`, `2004 DEPT C2004A01274.doc`, +5 |
| 2005 | `2005 DEPT C2004A01325.doc`, `2005 DEPT C2005A00037.doc`, +5 |
| 2006 | `2006 DEPT C2005A00074.doc`, `2006 No1 C2012C00814.doc`, +5 |
| 2007 | `2007 DEPT C2006A00068.doc`, `2007 No1 C2012C00879.doc`, +4 |
| 2008 | `2008 DEPT C2007A00097.doc`, `2008 No1 C2012C00882.doc`, +5 |
| 2009 | `2009 DEPT C2008A00057.doc`, `2009 No2 C2010C00629.doc`, +3 |
| 2010 | `2010 DEPT C2009A00065.doc`, `2010 No1 C2012C00874.doc`, +3 |
| 2011 | `2011 DEPT C2010A00062.doc`, `2011 No1 C2012C00506.doc`, `2011 No3 C2012C00658.doc` |
| 2012 | `2012 DEPT C2012C00443.doc`, `2012 No4 C2012C00651.doc`, +2 |
| 2013 | `2013 DEPT C2012A00080.doc` |

**Preguntas a responder:**
- ¿Los archivos `.doc` abren correctamente en Word/LibreOffice?
- Los años 2000 y 2001 aparecen como gaps en la gráfica — ¿son los únicos que deberían tener datos o hay años intermedios también vacíos?
- ¿Hay una agencia de I+D consistente en Australia que debería aparecer cada año (ej. CSIRO, ARC)?

---

### Colombia — 1995, 1999, 2000, 2001, 2011

| Año  | Archivo |
|------|---------|
| 1995 | `1995 ley-168-de-1994.doc` |
| 1999 | `1999 LEY 547 DE 1999.doc` |
| 2000 | `2000 LEY 547 DE 1999.doc` (misma ley que 1999 — ¿correcto?) |
| 2001 | `2001 LEY 628 DE 2000.doc` |
| 2011 | `2011 LEY 1420 DE 2010.doc` |

**Preguntas a responder:**
- Los años 1999 y 2000 referencian el mismo archivo (`LEY 547 DE 1999`). ¿Es correcto que la misma ley cubra dos años fiscales?
- ¿Estos documentos `.doc` contienen tablas presupuestales con líneas de Colciencias/SENA u otras agencias de I+D?

---

## Tipo B — Bug de código (`BudgetRow.get`)
**Error:** `'BudgetRow' object has no attribute 'get'` o `'str' object has no attribute 'get'`
**Diagnóstico:** El pipeline extrajo datos parcialmente pero falló durante el post-procesamiento
al intentar llamar `.get()` sobre un objeto `BudgetRow` tipado en lugar de un dict.
**Importante:** Puede haber datos parciales en `docx_results.csv` para estos países/años.

### Canada — 1987–1995 (BudgetRow bug) y 1996–2003 (RateLimitError)

| Años (BudgetRow bug) | Años (Rate limit) |
|----------------------|-------------------|
| 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995 | 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003 |
| 2006, 2007, 2008, 2009, 2020, 2022 | — |

Archivos de muestra: `1987-3.pdf`, `1988-1.pdf`, `Appropriation Act 2006-07 C38.pdf`

**Preguntas a responder:**
- ¿Existe `Data/output/budget/Canada/canada_docx_results.csv`? Si sí, ¿tiene filas para estos años?
- Los años 1987-1995 tienen archivos nombrados como `YYYY-N.pdf` — ¿qué tipo de documento es este para Canadá?

### Japan — 2000–2024 (TODOS los años fallaron)

Todos los archivos siguen el patrón `YYYY DL{YYYY}11001.pdf` (ej. `2000 DL200011001.pdf`).

- Años 2000–2009: solo `RateLimitError`
- Años 2010–2024: combinación de `BudgetRow.get` + `RateLimitError`

**Preguntas a responder:**
- Japón tiene 0 datos en toda la base — ¿hay algún año anterior a 2000 que sí funcionó?
- Los PDFs `DL{YEAR}11001.pdf` parecen ser del Ministerio de Finanzas de Japón. ¿Están en japonés? Si es así, ¿el pipeline tiene soporte para japonés?
- ¿Existe `Data/output/budget/Japan/japan_docx_results.csv`?

### Italy — 1990

| Año | Archivos | Error |
|-----|----------|-------|
| 1990 | `1990 19891230_303_SO_098-1.pdf` | `'str' object has no attribute 'get'` |

**Preguntas a responder:**
- ¿Hay datos de Italy para años cercanos (1989, 1991) en la base?
- ¿El archivo PDF de 1990 se puede abrir?

---

## Tipo C — Rate Limit de API (re-correr)
**Error:** `RetryError[RateLimitError]` o `RetryError[APIConnectionError]`
**Diagnóstico:** El pipeline intentó procesar el archivo pero agotó los reintentos por límite de API.
**No hay problema con los documentos — solo hay que volver a correr.**

| País | Años afectados |
|------|---------------|
| Iceland | 2022, 2023, 2024, 2025 |
| Sweden | 1975, 1980–1991, 1993 |
| Italy | 2025 |
| Japan | 2000–2009 (también tienen BudgetRow en 2010+) |
| Canada | 1996–2003 |

**Acción:** Simplemente re-correr con `--fresh`:
```bash
python3 -m budget.pipeline --countries Iceland --years 2022-2025 --fresh
python3 -m budget.pipeline --countries Sweden --years 1975-1993 --fresh
python3 -m budget.pipeline --countries Italy --years 2025-2025 --fresh
```

---

## Tipo D — Problemas de archivo

### Belgium 2000 — PDF vacío
**Error:** `Cannot open empty file`
**Archivo:** `2000 Belgium 50K0198001.pdf`
**Preguntas:** ¿El archivo tiene 0 bytes? ¿Hay una versión alternativa del presupuesto belga 2000?

### Switzerland 2003 — PDF dañado
**Error:** `code=7: cannot find page 6 in page tree`
**Archivo:** `2003-VA3-d.pdf`
**Preguntas:** ¿El PDF abre normalmente? ¿Solo falla en la página 6? ¿Hay otra versión descargable?

---

## Output esperado de esta revisión

Por cada grupo, responder en este formato:

```
### [País] [Año(s)]
- **¿Documentos legibles?** Sí / No / Parcial
- **¿Contiene datos de I+D?** Sí / No / No está claro
- **Causa del gap:** [formato .doc | bug código | rate limit | archivo dañado | documento sin I+D]
- **Acción recomendada:** [convertir .doc | re-correr pipeline | arreglar código | buscar fuente alternativa | marcar como sin datos]
- **Notas adicionales:** ...
```

---

## Comandos útiles para inspección rápida

```bash
# Ver qué archivos existen para un país
ls Data/input/finance_bills/Australia/ | grep "2000\|2001"

# Ver si hay datos parciales en el CSV de resultados
head -5 Data/output/budget/Australia/australia_docx_results.csv

# Verificar tamaño de un PDF sospechoso
ls -lh "Data/input/finance_bills/Belgium/2000 Belgium 50K0198001.pdf"

# Intentar convertir .doc a .docx manualmente (requiere LibreOffice)
/opt/homebrew/bin/soffice --headless --convert-to docx \
  --outdir "Data/input/finance_bills/Australia/" \
  "Data/input/finance_bills/Australia/2000 No1 C2008C00464.doc"
```

---

*Generado automáticamente desde `Data/output/budget/run_log.jsonl` — $(date +%Y-%m-%d)*
