# Spark Performance Benchmark: CSV vs. Parquet vs. Delta & Join Strategies

**Projekt im Rahmen des CAS Information Engineering - Modul Big Data**

---

## 📋 Leistungsnachweis - Projektanforderungen

### Bewertung
- **Bestanden / Nicht bestanden (Pass/Fail)** - basierend auf einer Projekt-Präsentation

### Projektanforderungen

#### 1. Team-Bildung
- Gruppen von 3 Personen (empfohlen)
- Teams mit 2 Personen möglich, aber brauchen Bestätigung
- **Start:** 15. Dezember 2025

#### 2. Projekt-Inhalt
- Implementierung eines Projekts **nach eigener Wahl** mit einem **distributed Big Data Framework**
- Mögliche Technologien: Spark (RDD/DataFrames), GPU-Support, Koalas/Pandas on Spark, etc.
- Verwendung eines **Schweizer Datensatzes** ODER Daten aus eurem eigenen Geschäft
- Daten müssen "Big Data"-Kriterien erfüllen
- Kann auch Real-World-Datenakquisition beinhalten
- **Frei wählbares Thema** - "boldly go where noone went before..."

#### 3. Abgabe bis Sonntag, 11. Januar 2026 @ 23:59 Uhr
- Analysis Code (Python/PySpark Code)
- Referenz zum verwendeten Datensatz
- **PDF-Slides der Präsentation (max. 10 Slides)**
- Upload auf Moodle

#### 4. Projekt-Präsentationen
- **Datum:** 12. Januar 2026

### Zeitplan
- **01.12.2025:** Project Outline vorgestellt
- **15.12.2025:** Projektstart & Implementation
- **05.01.2026:** Weiter Implementation
- **12.01.2026:** Projekt-Präsentationen

---

## 1. Projektbeschreibung

Dieses Projekt untersucht die Performance-Unterschiede moderner Big Data Dateiformate und Optimierungsstrategien in Apache Spark. Ziel ist es, quantitativ zu belegen, wie sich physikalische Speicherformate (CSV, Parquet, Delta Lake) und logische Ausführungspläne (Join-Strategien) auf die Laufzeit von ETL-Prozessen auswirken.

### Projektziel: "Spark vs. The World" – Ein Optimierungs-Showcase

Der Benchmark konzentriert sich auf zwei Hauptaspekte:

1. **I/O-Effizienz:** Vergleich von Speicherplatzbedarf und Lesegeschwindigkeit zwischen row-oriented (CSV) und column-oriented (Parquet) Formaten, sowie die Auswirkungen von Kompression (Snappy).

2. **Query Optimization:** Analyse der Auswirkungen verschiedener Join-Typen (Sort-Merge Join vs. Broadcast Hash Join) und der Einfluss von Partitioning (Z-Ordering) auf die Query-Performance.

Die Ergebnisse dienen als Best-Practice-Leitfaden für Data Engineering Pipelines in Spark-Clustern.

### Technischer Ansatz
- **Datensatz:** Generierung eines riesigen synthetischen Datensatzes (z.B. 100 GB+ an Transaktionsdaten) oder Nutzung eines Standard-Benchmark (TPC-H)
- **Fokus:** Technik und Performance, nicht Business-Insights
- **Vergleiche:** CSV vs. Parquet vs. Delta Lake
- **Join-Strategien:** Broadcast Hash Join vs. Sort-Merge Join (erzwungen via Code)
- **Query Plan Analyse:** Logical vs. Physical Plan
- **Big Data Tech:** Spark Catalyst Optimizer, File Formats, Tuning Parameter
- **Komplexität:** Mittel (aber sehr lehrreich für die Prüfung/Verständnis)

---

## 2. Ordnerstruktur

Die Projektstruktur folgt Standard-Konventionen für Data Science Projekte:

```text
├── data/
│   ├── raw/                  # Rohdaten (oder Skript zur Generierung)
│   ├── processed/            # Konvertierte Daten (Parquet/Delta) - wird meist in .gitignore aufgenommen
│   └── dictionary/           # Metadaten oder Lookup-Tables
│
├── notebooks/
│   ├── 01_data_generation.ipynb   # Generierung synthetischer Big Data (TPC-H style)
│   ├── 02_format_benchmark.ipynb  # Vergleiche CSV vs. Parquet Lese-/Schreibzeiten
│   └── 03_join_optimization.ipynb # Join Strategien & Z-Order Tests
│
├── src/
│   ├── benchmark_utils.py    # Hilfsfunktionen für Timing und Logging
│   └── config.py             # Konfiguration (Pfade, Cluster-Settings)
│
├── results/
│   ├── benchmark_logs.csv    # Rohdaten der Laufzeitmessungen
│   └── plots/                # Generierte Grafiken für die Präsentation
│
├── requirements.txt          # Python Abhängigkeiten
└── README.md                 # Projekt-Dokumentation
```

---

## 3. Requirements Engineering

### 3.1 Funktionale Anforderungen

- **Datengenerierung:** Das System muss in der Lage sein, synthetische Datensätze in konfigurierbarer Größe (z.B. 1GB, 10GB, 50GB) zu erzeugen, um "Big Data" Szenarien zu simulieren.

- **Format-Konvertierung:** Fähigkeit, Daten in CSV, Parquet und Delta Lake Formate zu schreiben.

- **Query-Ausführung:** Implementierung von Standard-Queries (Filter, Aggregation, Join), die gegen alle Formate ausgeführt werden.

- **Messung & Logging:** Automatisierte Erfassung von Metriken:
  - Ausführungszeit (Execution Time)
  - Datenvolumen auf der Festplatte (Storage Size)
  - Shuffle Read/Write Size (aus Spark Listenern)

### 3.2 Nicht-Funktionale Anforderungen

- **Skalierbarkeit:** Der Benchmark muss auf einem verteilten Cluster (z.B. SwissProc oder Databricks Community Edition) lauffähig sein.

- **Reproduzierbarkeit:** Die Tests müssen durch Seeds in der Datengenerierung deterministische Ergebnisse liefern.

- **Analysierbarkeit:** Nutzung der Spark UI zur Validierung der Physical Plans (sicherstellen, dass z.B. wirklich ein Broadcast Join stattgefunden hat).

---

## 4. Systemarchitektur und Design

### 4.1 High-Level Architektur

Das System ist als Experiment-Pipeline konzipiert, die auf Apache Spark aufsetzt:

1. **Data Generator (Spark Job):** Erzeugt zwei große DataFrames (z.B. `Sales` und `Customers`) mit hoher Kardinalität.

2. **Storage Layer (HDFS/S3/Local):** Speichert diese DataFrames in drei Versionen ab:
   - `Raw (CSV)`: Unkomprimiert, Row-based
   - `Optimized (Parquet)`: Snappy compressed, Columnar
   - `Managed (Delta)`: Versioniert, mit Z-Order Index

3. **Benchmark Engine (Driver):** Iteriert durch definierte Testfälle:
   - **Test A:** Scan Performance (Full Scan vs. Filter Pushdown)
   - **Test B:** Join Performance (Force Broadcast vs. Sort-Merge)

4. **Reporting:** Aggregation der Ergebnisse mittels Pandas/Matplotlib

### 4.2 Design-Entscheidungen (Spark Internals)

Um die Unterschiede deutlich zu machen, werden wir gezielt in den Catalyst Optimizer eingreifen:

- **Broadcast Hints:** Wir verwenden `broadcast(df)` um Spark zu zwingen, kleinere Tabellen an alle Worker zu senden, um Shuffles zu vermeiden.

- **Deaktivierung von AQE:** Für faire Vergleiche testen wir teilweise mit `spark.sql.adaptive.enabled = false`, um zu sehen, wie Spark ohne adaptive Optimierung plant.

- **Caching:** Um I/O-Effekte von Rechen-Effekten zu trennen, werden spezifische Benchmarks mit `df.cache()` durchgeführt.

---

## 5. Ausführung (How to Run)

### Setup

1. **Umgebung aufsetzen:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Datengenerierung starten:**
   - Öffne `notebooks/01_data_generation.ipynb`
   - **Vorsicht:** Hoher Speicherplatzbedarf!
   - Konfiguriere die gewünschte Datengröße

3. **Benchmarks ausführen:**
   - `notebooks/02_format_benchmark.ipynb` - Format-Vergleiche
   - `notebooks/03_join_optimization.ipynb` - Join-Strategien
   - Die Ergebnisse werden automatisch im Ordner `results/` gespeichert

4. **Ergebnisse analysieren:**
   - Die Benchmark-Logs finden sich in `results/benchmark_logs.csv`
   - Visualisierungen werden in `results/plots/` abgelegt

---

## 6. Erwartete Ergebnisse

### Performance-Metriken
- Vergleich der Lesezeiten: CSV vs. Parquet vs. Delta
- Speicherplatz-Einsparungen durch Kompression
- Shuffle-Overhead bei verschiedenen Join-Strategien
- Auswirkungen von Z-Ordering auf Query-Performance

### Deliverables
- Python/PySpark Code (vollständig dokumentiert)
- Referenz zum verwendeten Datensatz
- **Präsentations-Slides (max. 10 Slides)**
- Benchmark-Ergebnisse und Visualisierungen

---

## 7. Team & Kontakt

- **Team-Größe:** 3 Personen
- **Projektstart:** 15. Dezember 2025
- **Abgabe:** 11. Januar 2026, 23:59 Uhr
- **Präsentation:** 12. Januar 2026

---

## 8. Technologie-Stack

- **Apache Spark 3.x** (PySpark)
- **Delta Lake** (für transaktionale Speicherung)
- **Python 3.8+**
- **Jupyter Notebooks**
- **Pandas & Matplotlib** (für Reporting)
- **Optional:** Databricks Community Edition oder SwissProc Cluster

---

## 9. Lizenz & Hinweise

Dieses Projekt dient ausschließlich zu Bildungszwecken im Rahmen des CAS Information Engineering.

---

**"Boldly go where no one went before..."** 🚀
