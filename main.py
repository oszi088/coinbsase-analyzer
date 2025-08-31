# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import csv
import time
from coinbase.rest import RESTClient

# REST client inicializálása
client = RESTClient(
    api_key="organizations/518141ff-d51e-42dc-a850-0dc2010a848b/apiKeys/6503d711-6871-4f79-b480-7cf170950de1",
    api_secret="-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEIOeJeVKo6fOj11RhtlMupl5Rrvd66QkzgkpE/8rtWhzooAoGCCqGSM49\nAwEHoUQDQgAEGSkxVhHyItqGuLYLhmGzZQnhsMXMI4ZcZqyI3o9MeATkruoiqhPM\nYaHU9nYizFjI9Jru3iLiMvouRCRWdmHYSA==\n-----END EC PRIVATE KEY-----\n"
)

# Paraméterek megadása
product_id = "ETH-USD"  # vagy más altcoin pár
start = int(time.time()) - 86400  # 24 óraja, UNIX timestamp
end = int(time.time())
granularity = "ONE_HOUR"

# Gyertyás adatok lekérése
response = client.get_candles(
    product_id=product_id,
    start=str(start),
    end=str(end),
    granularity=granularity
)

# Válasz JSON formátumban: lista OHLCV adatokról
candles = response.to_dict().get('candles', [])

# CSV-be mentés
filename = f"{product_id}_candles_{start}_{end}.csv"
with open(filename, mode="w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["start", "low", "high", "open", "close", "volume"])
    for c in candles:
        writer.writerow([c["start"], c["low"], c["high"], c["open"], c["close"], c["volume"]])

print(f"Candlestick data elmentve ide: {filename}")


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
