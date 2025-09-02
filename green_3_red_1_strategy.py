import pandas as pd
import glob
import os
from coinbase.rest import RESTClient

# === Coinbase API kliens (ha valódi tradet is akarsz) ===
# client = RESTClient(api_key="YOUR_API_KEY", api_secret="YOUR_API_SECRET")

def load_all_coins(data_dir: str) -> dict:
    all_data = {}
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    for file in csv_files:
        df = pd.read_csv(file)
        product_id = df["product_id"].iloc[0]
        all_data[product_id] = df
    return all_data

def simulate_strategy(df: pd.DataFrame, initial_balance=1000.0, taker_fee=0.005):
    """
    Szimulálja a BUY/SELL stratégiát.
    Taker fee minden tranzakciónál (pl. 0.005 = 0.5%)
    Visszaadja a profitot és a teljes fee-t is.
    """
    balance_usdc = initial_balance
    balance_coin = 0.0
    last_action = "SELL"  # kezdetben USDC-ben vagyunk
    total_fee_paid = 0.0

    for i in range(3, len(df)):
        candle = df.iloc[i]
        close_price = candle["close"]

        # Stratégiák
        last_3_green = all(df.iloc[i-j]["close"] > df.iloc[i-j]["open"] for j in range(1, 4))
        last_1_red = candle["close"] < candle["open"]

        action = None
        if last_3_green:
            action = "BUY"
        elif last_1_red:
            action = "SELL"

        # Döntés
        if action == "BUY" and last_action != "BUY":
            # BUY all, fee levonva
            fee_amount = balance_usdc * taker_fee
            total_fee_paid += fee_amount
            amount_to_spend = balance_usdc - fee_amount
            balance_coin = amount_to_spend / close_price
            balance_usdc = 0.0
            last_action = "BUY"

        elif action == "SELL" and last_action != "SELL":
            # SELL all, fee levonva
            proceeds = balance_coin * close_price
            fee_amount = proceeds * taker_fee
            total_fee_paid += fee_amount
            balance_usdc = proceeds - fee_amount
            balance_coin = 0.0
            last_action = "SELL"

    final_value = balance_usdc + balance_coin * df.iloc[-1]["close"]
    profit_usd = final_value - initial_balance
    profit_pct = (final_value / initial_balance - 1) * 100

    return {
        "final_value": final_value,
        "profit_pct": profit_pct,
        "profit_usd": profit_usd,
        "total_fee_paid": total_fee_paid
    }

def run_backtest(data_dir: str, output_csv: str, taker_fee=0.005):
    all_data = load_all_coins(data_dir)
    results = []

    for product_id, df in all_data.items():
        result = simulate_strategy(df, taker_fee=taker_fee)
        results.append({
            "product_id": product_id,
            "final_value": result["final_value"],
            "profit_pct": result["profit_pct"],
            "profit_usd": result["profit_usd"],
            "total_fee_paid": result["total_fee_paid"]
        })

    results_df = pd.DataFrame(results)
    results_df.to_csv(output_csv, index=False)

    # Összesítés
    num_positive = (results_df["profit_usd"] > 0).sum()
    num_negative = (results_df["profit_usd"] <= 0).sum()
    total_fees = results_df["total_fee_paid"].sum()

    print(f"Összesen {len(results_df)} coin:")
    print(f"  Pluszos: {num_positive}")
    print(f"  Minuszos: {num_negative}")
    print(f"  Teljes fee fizetve: {total_fees:.2f} USDC")

    return results_df

# === Használat ===
# results = run_backtest("./coin_data", "strategy_results_with_fee.csv", taker_fee=0.005)
# print(results)
