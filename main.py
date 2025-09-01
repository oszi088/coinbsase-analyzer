from coinbase.rest import RESTClient
import os
import time
import pandas as pd
from datetime import datetime, timezone

# ===== CONFIG =====
START_DATE = 1420070400 # 2015.01.01. 00:00:00 UTC
QUOTE_CURRENCY = "USD"
TOP_ALTCOIN_COUNT = 100
EXPLICIT = ["BTC", "ETH", "ARB", "GLMR"]
SLEEP_BETWEEN_CALLS = 0.25


def get_products(client: RESTClient):
    """Lekéri az összes spot terméket."""
    products = []
    resp = client.get_products(limit=500, get_all_products=True)
    products.extend(resp["products"])
    return products


def pick_top_alts(products, top_n=100, quote="USD"):
    """Top altcoinok kiválasztása USD könyv volumene alapján (BTC/ETH kihagyva)."""
    rows = []
    for p in products:
        if p.product_type != "SPOT":
            continue
        pid = p.product_id
        if not pid or not pid.endswith(f"-{quote}"):
            continue
        base = pid.split("-")[0]
        vol = None
        try:
            vol = float(p.volume_24h)
        except Exception:
            vol = 0.0
        rows.append((base, pid, vol))

    # Minden base-hoz a legnagyobb volumenű USD könyv
    best_by_base = {}
    for base, pid, vol in rows:
        if base not in best_by_base or vol > best_by_base[base][2]:
            best_by_base[base] = (base, pid, vol)

    # BTC, ETH kihagyása a "top alt" listából
    alt_bases = [b for b in best_by_base if b not in ["BTC", "ETH"]]
    alt_bases_sorted = sorted(
        alt_bases, key=lambda b: best_by_base[b][2], reverse=True
    )
    return [best_by_base[b][1] for b in alt_bases_sorted[:top_n]]


def fetch_candles(client: RESTClient, product_id: str, start: str, end: str):
    """Letölti az adott termék napi gyertyáit SDK-val."""
    candles = []
    resp = client.get_candles(
        product_id=product_id, start=start, end=end, granularity="ONE_DAY", limit=350
    )
    candles.extend(resp["candles"])
    return candles


def normalize_candles(candles, product_id):
    """DataFrame-et csinál az API candle-ból."""
    df = pd.DataFrame(candles)
    if df.empty:
        return df
    df.columns = ["start", "low", "high", "open", "close", "volume"]
    df["time"] = pd.to_datetime(df["start"], unit="s", utc=True)
    df["product_id"] = product_id
    df = df.sort_values("time").reset_index(drop=True)
    return df


def main():
    # api_key = os.getenv("COINBASE_API_KEY")
    # api_secret = os.getenv("COINBASE_API_SECRET")
    client = RESTClient(
        api_key="organizations/518141ff-d51e-42dc-a850-0dc2010a848b/apiKeys/6503d711-6871-4f79-b480-7cf170950de1",
        api_secret="-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEIOeJeVKo6fOj11RhtlMupl5Rrvd66QkzgkpE/8rtWhzooAoGCCqGSM49\nAwEHoUQDQgAEGSkxVhHyItqGuLYLhmGzZQnhsMXMI4ZcZqyI3o9MeATkruoiqhPM\nYaHU9nYizFjI9Jru3iLiMvouRCRWdmHYSA==\n-----END EC PRIVATE KEY-----\n"
    )

    print("Fetching product list...")
    products = get_products(client)
    print(f"Total products: {len(products)}")

    # Top altcoinok
    top_alts = pick_top_alts(products, TOP_ALTCOIN_COUNT, QUOTE_CURRENCY)
    explicit_pids = [f"{b}-USD" for b in EXPLICIT]
    all_pids = list(dict.fromkeys(explicit_pids + top_alts))
    print(f"Total selected assets: {len(all_pids)}")

    for pid in all_pids:
        print(f"Downloading candles for {pid}...")
        candles = fetch_candles(client, pid, str(START_DATE), str(int(time.time())))
        if not candles:
            print(f"  No candles for {pid}")
            continue
        df = normalize_candles(candles, pid)
        fname = pid.replace("-", "_") + "_1d.csv"
        df.to_csv(fname, index=False)
        print(f"  Saved {len(df)} rows -> {fname}")
        time.sleep(SLEEP_BETWEEN_CALLS)


if __name__ == "__main__":
    main()