# ⚡ Spark Performance Benchmark - Setup

Diese Anleitung beschreibt die Installation und Ausführung des Projekts unter **Windows**.

# Hinweise:
# - Für das Hadoop-Setup erwartet dieses Makefile die Dateien: 
#     C/hadoop/winutils.exe
#     C/hadoop/hadoop.dll
#     - Die Dateien befinden sich auch im GitHub Projekt.
# - Umgebungsvariablen dauerhaft setzen (HADOOP_HOME + Path) erfordert Admin-Rechte
#   und ist hier als PowerShell-Befehl (optional) enthalten.


## 1. Verwendete Versionen

Das Projekt wurde erfolgreich mit folgender Konfiguration getestet:

* **OS:** Windows 10/11
* **Python:** 3.13.7
* **Java:** OpenJDK 17.0.17
* **Spark:** PySpark 3.5.0
* **Delta Lake:** delta-spark 3.0.0

## 2. Installation

Es wird empfohlen, ein virtuelles Environment zu nutzen. Öffne ein Terminal (PowerShell) im Projektordner:

### 1. Environment erstellen & aktivieren
python -m venv venv

### 2. Aktivieren
.\venv\Scripts\Activate.ps1

### 3. Dependencies installieren
pip install -r requirements.txt

### 4. Hadoop Test
python test_hadoop_config.py

### 5. Jupyter starten
jupyter lab
