import os
import time
import json
from datetime import datetime
from typing import List, Dict

import pandas as pd
import yfinance as yf


def load_categories() -> Dict[str, List[str]]:
    cfg_path = os.path.join("config", "categories.json")
    if not os.path.exists(cfg_path):
        raise FileNotFoundError("config/categories.json não encontrado.")
    with open(cfg_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_tickers_and_category() -> List[Dict[str, str]]:
    categories = load_categories()
    rows: List[Dict[str, str]] = []
    for cat, syms in categories.items():
        for s in syms:
            rows.append({"symbol": s, "category": cat})
    return rows


def collect_prices(tickers: List[str]) -> pd.DataFrame:
    # Baixa em blocos para reduzir risco de bloqueios
    chunks = []
    batch_size = 5
    start = "2000-01-01"
    end = datetime.utcnow().strftime("%Y-%m-%d")

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]
        data = yf.download(
            tickers=batch,
            start=start,
            end=end,
            interval="1d",
            group_by="ticker",
            threads=True,
            auto_adjust=False,
            progress=False,
        )

        # Normaliza o formato: um DataFrame por ticker
        # Quando múltiplos tickers, yfinance retorna MultiIndex por coluna
        if isinstance(data.columns, pd.MultiIndex):
            for t in batch:
                if t in data.columns.levels[0]:
                    df_t = data[t].copy()
                    df_t = df_t.reset_index()
                    df_t["symbol"] = t
                    chunks.append(df_t)
        else:
            # Caso apenas um ticker, sem MultiIndex
            df_t = data.copy().reset_index()
            df_t["symbol"] = batch[0]
            chunks.append(df_t)
        # Pequena pausa entre chamadas para evitar rate limiting
        time.sleep(2)

    if not chunks:
        raise RuntimeError("Nenhum dado retornado do Yahoo Finance.")

    df = pd.concat(chunks, ignore_index=True)
    df.rename(columns={"Date": "date"}, inplace=True)
    # Ordena para consistência
    df.sort_values(["symbol", "date"], inplace=True)
    return df


def save_csv(df: pd.DataFrame) -> str:
    os.makedirs(os.path.join("data", "raw"), exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join("data", "raw", f"yahoo_prices_{ts}.csv")
    df.to_csv(out_path, index=False)
    return out_path


def main() -> None:
    sym_rows = load_tickers_and_category()
    tickers = [r["symbol"] for r in sym_rows]
    cat_map = {r["symbol"]: r["category"] for r in sym_rows}
    print(f"Coletando {len(tickers)} tickers do Yahoo Finance...")
    df = collect_prices(tickers)
    # Anexa a categoria
    df["category"] = df["symbol"].map(cat_map).fillna("uncategorized")
    out_path = save_csv(df)
    print(f"Dataset salvo em: {out_path}")


if __name__ == "__main__":
    main()