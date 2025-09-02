import candle_service
import green_3_red_1_strategy
if __name__ == "__main__":
    #candle_service.get_candle_histories()
    green_3_red_1_strategy.run_backtest("./candles/1d", "./strategies/green_3_red_1_strategy/results.csv")
