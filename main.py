from coinbase.rest import RESTClient
import os
import time
import pandas as pd

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


def fetch_candles(client: RESTClient, product_id: str, start_ts: int, end_ts: int):
    """
    Lekéri a gyertyákat 350 napos szeletekben start_ts és end_ts között.
    start_ts, end_ts: Unix timestamp (int)
    """
    SECS_PER_DAY = 24 * 60 * 60
    MAX_DAYS = 350
    MAX_RANGE = MAX_DAYS * SECS_PER_DAY  # 350 nap másodpercben

    all_candles = []
    current_end = end_ts

    while current_end > start_ts:
        # Szelet kezdete (max 350 nappal vissza)
        current_start = max(start_ts, current_end - MAX_RANGE)

        # API hívás
        resp = client.get_candles(
            product_id=product_id,
            start=str(current_start),
            end=str(current_end),
            granularity="ONE_DAY",
            limit=350,
        )
        candles = resp.candles
        all_candles.extend(candles)

        # Következő iteráció
        current_end = current_start
        time.sleep(0.25)  # rate limit védelem

    return all_candles


def normalize_candles(candles, product_id):
    """DataFrame-et csinál az API candle-listából."""
    if not candles:
        return pd.DataFrame()

    # candles már dict-ek listája kulcsokkal: start, low, high, open, close, volume
    df = pd.DataFrame(candles)

    # Biztosítsuk a mezők sorrendjét
    expected_cols = ["start", "low", "high", "open", "close", "volume"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None  # ha hiányzik, töltsük fel

    df = df[expected_cols]

    # start timestamp konvertálás
    df["time"] = pd.to_datetime(df["start"], unit="s", utc=True)
    df["product_id"] = product_id

    return df.sort_values("time").reset_index(drop=True)


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
        candles = fetch_candles(client, pid, START_DATE, int(time.time()))
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