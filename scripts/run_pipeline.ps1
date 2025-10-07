Param(
  [string]$SymbolsFile = "data\symbols.txt"
)

Write-Host "Subindo serviços com docker compose..." -ForegroundColor Cyan
docker compose up -d

Write-Host "Instalando dependências Python..." -ForegroundColor Cyan
pip install -r requirements.txt

Write-Host "Executando coletor Yahoo Finance..." -ForegroundColor Cyan
python .\src\collect\yahoo_finance_collector.py

$latest = Get-ChildItem -Path .\data\raw -Filter "yahoo_prices_*.csv" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $latest) {
  Write-Error "Nenhum arquivo gerado pela coleta foi encontrado em data/raw."
  exit 1
}
Write-Host ("Arquivo mais recente: " + $latest.FullName) -ForegroundColor Green

Write-Host "Copiando CSV para o container do NameNode..." -ForegroundColor Cyan
docker cp $latest.FullName namenode:/tmp/

Write-Host "Enviando CSV ao HDFS..." -ForegroundColor Cyan
docker exec namenode hdfs dfs -mkdir -p /datasets/yahoo_finance/raw
docker exec namenode hdfs dfs -put -f /tmp/$($latest.Name) /datasets/yahoo_finance/raw/

Write-Host "Submetendo job Spark de curadoria..." -ForegroundColor Cyan
docker exec -e DATASETS_BASE=/datasets/yahoo_finance -e CATEGORIES_PATH=/opt/config/categories.json spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/curate_yahoo_finance.py

Write-Host "Gerando janelas de eventos (±60 dias)..." -ForegroundColor Cyan
docker exec -e DATASETS_BASE=/datasets/yahoo_finance -e EVENTS_PATH=/opt/config/events.json spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/event_windows.py

Write-Host "Pipeline concluído. Dados curados em HDFS: /datasets/yahoo_finance/curated/prices" -ForegroundColor Green
Write-Host "Event windows armazenadas em: /datasets/yahoo_finance/curated/event_windows" -ForegroundColor Green

# Exporta dados de event windows do HDFS para uso em notebooks (local)
Write-Host "Exportando event windows do HDFS para data/exports..." -ForegroundColor Cyan
New-Item -ItemType Directory -Force -Path .\data\exports | Out-Null
docker exec namenode bash -lc "rm -rf /tmp/export && mkdir -p /tmp/export"
docker exec namenode hdfs dfs -get -f -p /datasets/yahoo_finance/curated/event_windows /tmp/export/event_windows
docker cp namenode:/tmp/export/event_windows .\data\exports\event_windows
Write-Host "Export concluído: data/exports/event_windows" -ForegroundColor Green