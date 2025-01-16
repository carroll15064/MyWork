import pandas as pd

def mean_reversion_strategy(prices: pd.Series, window: int = 20) -> pd.Series:
    """
    Implements a mean reversion strategy using z-scores.
    
    Args:
        prices (pd.Series): Price series data
        window (int): Rolling window for calculating mean and standard deviation
    
    Returns:
        pd.Series: Trading signals (1 for buy, -1 for sell, 0 for hold)
    """
    rolling_mean = prices.rolling(window=window).mean()
    rolling_std = prices.rolling(window=window).std()
    z_score = (prices - rolling_mean) / rolling_std
    signals = (z_score < -1).astype(int) - (z_score > 1).astype(int)
    return signals 