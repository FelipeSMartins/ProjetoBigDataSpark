Write-Host "Instalando PySpark localmente..." -ForegroundColor Cyan
pip install pyspark==3.5.3

Write-Host "Executando coletor Yahoo Finance (local)..." -ForegroundColor Cyan
python .\src\collect\yahoo_finance_collector.py

Write-Host "Executando curadoria com PySpark (local)..." -ForegroundColor Cyan
python .\jobs\spark\curate_yahoo_finance.py

Write-Host "Gerando janelas de eventos com PySpark (local)..." -ForegroundColor Cyan
python .\jobs\spark\event_windows.py

Write-Host "Pronto. Dados curados: data/exports/curated/prices" -ForegroundColor Green
Write-Host "Event windows: data/exports/event_windows" -ForegroundColor Green