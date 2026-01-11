# 🚀 Spark Performance Benchmark: CSV vs. Parquet vs. Delta

**Projekt im Rahmen des CAS Information Engineering - Modul Big Data**

---

## 👥 Team
* **Dörig Tobias**
* **Shala Ismet**
* **Vonbank Samuel**

---

## 1. Projektbeschreibung

Dieses Projekt untersucht die Performance-Unterschiede moderner Big Data Dateiformate und Optimierungsstrategien in Apache Spark. Ziel ist es, quantitativ zu belegen, wie sich physikalische Speicherformate (CSV, Parquet, Delta Lake) und logische Ausführungspläne (Join-Strategien) auf die Laufzeit von ETL-Prozessen auswirken.

### 1.1 Projektziel: "Spark vs. The World"
Der Benchmark konzentriert sich auf zwei Hauptaspekte:

1.  **I/O-Effizienz:** Vergleich von Speicherplatzbedarf und Lesegeschwindigkeit zwischen row-oriented (CSV) und column-oriented (Parquet/Delta) Formaten sowie die Auswirkungen von Kompression (Snappy).
2.  **Query Optimization:** Analyse der Auswirkungen verschiedener Join-Typen (Sort-Merge Join vs. Broadcast Hash Join) und der Einfluss von Partitioning (Z-Ordering) auf die Query-Performance.

Die Ergebnisse dienen als Best-Practice-Leitfaden für Data Engineering Pipelines.

### 1.2 Verwendeter Datensatz (PaySim)
Anstatt rein zufällige Daten zu generieren, verwenden wir den **PaySim 1 (Mobile Money Transactions)** Datensatz.

* **Quelle:** [Kaggle - PaySim1 Mobile Money Transactions](https://www.kaggle.com/datasets/ealaxi/paysim1)
* **Inhalt:** Synthetische Finanzdaten (Simulation von Mobile Money Transaktionen).
* **Volumen:** Ca. 6.3 Millionen Transaktionen.
* **Relevanz:** Der Datensatz bietet eine realistische Schema-Struktur (Transaktionen, Accounts, Salden) und eignet sich ideal, um Joins zwischen Fakten- und Dimensionstabellen zu simulieren.

---

## 2. Systemarchitektur und Design

### 2.1 High-Level Architektur
Das System ist als Experiment-Pipeline konzipiert, die auf Apache Spark (Local Mode) aufsetzt:

1.  **Data Ingestion (Notebook 01):** Lädt die PaySim-Rohdaten, bereinigt sie und erstellt zwei relationale Tabellen (`fact_transactions` und `dim_accounts`).
2.  **Storage Layer:** Speichert diese Tabellen in drei Versionen ab:
    * `CSV`: Unkomprimiert, Row-based (Referenzwert).
    * `Parquet`: Snappy compressed, Columnar (Standard).
    * `Delta Lake`: Versioniert, optimiert mit **Z-Ordering** (High-Performance).
3.  **Benchmark Engine (Notebooks 02 & 03):** Iteriert durch definierte Testfälle:
    * **Test A:** Scan Performance (Full Scan vs. Filter Pushdown).
    * **Test B:** Join Performance (Force Broadcast vs. Sort-Merge).
4.  **Reporting:** Automatische Protokollierung der Laufzeiten in CSV-Dateien.

### 2.2 Design-Entscheidungen (Spark Internals)
Um die Unterschiede deutlich zu machen, greifen wir gezielt in den Catalyst Optimizer ein:

* **Broadcast Hints:** Wir verwenden `F.broadcast(df)`, um Spark zu zwingen, kleinere Tabellen an alle Worker zu senden, um teure Shuffles zu vermeiden.
* **Deaktivierung von AQE:** Für faire Vergleiche testen wir teilweise mit `spark.sql.adaptive.enabled = false`.
* **Z-Ordering:** Bei Delta Lake nutzen wir mehrdimensionale Clusterung (Z-Order), um Data Skipping zu maximieren.

---
