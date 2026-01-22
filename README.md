# DATEV PDF to CSV

## Deutsch

Tools zum Extrahieren von DATEV-Reports aus PDF und Export als CSV (und Excel
standardmaessig).

### Voraussetzungen

- Python 3.10+
- pdfplumber
- pandas
- openpyxl

Installation:

```bash
python -m pip install pdfplumber pandas openpyxl
```

### Skripte

#### pdf-to-csv_DATEV_Entwicklungsuebersicht.py

- Extrahiert die BWA "Jahresentwicklungsuebersicht"-Seite.
- Wenn `--page` nicht gesetzt ist, wird eine Seite gesucht, die
  "Entwicklungsuebersicht" enthaelt.
- Schreibt standardmaessig CSV und XLSX (mit `--no-excel` deaktivierbar).
- Optionale Strukturdatei zur Zeilenanzahl-Pruefung via
  `DATEV Struktur/DATEV_BWA_Struktur_Vorlage.csv` (oder `--structure`).
- Batch-Modus ueberspringt vorhandene Dateien und gibt INFO aus.

Beispiele:

```bash
python pdf-to-csv_DATEV_Entwicklungsuebersicht.py --pdf input/BWA_2025_12.pdf --out output/jahresentwicklung.csv
python pdf-to-csv_DATEV_Entwicklungsuebersicht.py --batch --input-dir input --output-dir output
```

#### pdf-to-csv_DATEV_SUSA.py

- Extrahiert die DATEV "Summen und Saldenliste".
- Wenn `--start-page`/`--end-page` fehlen, wird nach Seiten mit
  "Summen und Salden" gesucht und der erste/letzte Treffer als Bereich verwendet.
- Schreibt standardmaessig CSV und XLSX (mit `--no-excel` deaktivierbar).
- Batch-Modus ueberspringt vorhandene Dateien und gibt INFO aus.

Beispiele:

```bash
python pdf-to-csv_DATEV_SUSA.py --pdf input/your_file.pdf --out output/susa.csv
python pdf-to-csv_DATEV_SUSA.py --batch --input-dir input --output-dir output
```

### Standardverhalten

Wenn keine Parameter angegeben sind, laufen beide Skripte im Batch von
`input/` nach `output/`.

### Ausgabeformat

- CSV nutzt Semikolon (`;`) und UTF-8 mit BOM fuer Excel-Kompatibilitaet.
- XLSX-Ausgabe nutzt pandas/openpyxl und formatiert Kopfzeile, Zahlen und
  Spaltenbreiten automatisch.

## English

Tools to extract DATEV reports from PDF and export them as CSV (and Excel by
default).

### Requirements

- Python 3.10+
- pdfplumber
- pandas
- openpyxl

Install:

```bash
python -m pip install pdfplumber pandas openpyxl
```

### Scripts

#### pdf-to-csv_DATEV_Entwicklungsuebersicht.py

- Extracts the BWA "Jahresentwicklungsuebersicht" page.
- If `--page` is not provided, it searches for a page containing
  "Entwicklungsuebersicht".
- Writes CSV and formatted XLSX by default (use `--no-excel` to disable XLSX).
- Optional structure file for row-count validation via
  `DATEV Struktur/DATEV_BWA_Struktur_Vorlage.csv` (or `--structure`).
- Batch mode skips files that already exist and prints INFO.

Examples:

```bash
python pdf-to-csv_DATEV_Entwicklungsuebersicht.py --pdf input/BWA_2025_12.pdf --out output/jahresentwicklung.csv
python pdf-to-csv_DATEV_Entwicklungsuebersicht.py --batch --input-dir input --output-dir output
```

#### pdf-to-csv_DATEV_SUSA.py

- Extracts the DATEV "Summen und Saldenliste".
- If `--start-page`/`--end-page` are omitted, it searches for pages titled
  "Summen und Salden" and uses the first/last match as the range.
- Writes CSV and formatted XLSX by default (use `--no-excel` to disable XLSX).
- Batch mode skips files that already exist and prints INFO.

Examples:

```bash
python pdf-to-csv_DATEV_SUSA.py --pdf input/your_file.pdf --out output/susa.csv
python pdf-to-csv_DATEV_SUSA.py --batch --input-dir input --output-dir output
```

### Default behavior

If no parameters are provided, both scripts run a batch conversion from
`input/` to `output/`.

### Output format

- CSV uses semicolon (`;`) and UTF-8 with BOM for Excel compatibility.
- XLSX output uses pandas/openpyxl and formats header, numbers, and column
  widths automatically.
