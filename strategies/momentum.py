import pandas as pd
import numpy as np

def momentum_strategy(data_fetcher, ib_connector, risk_manager, params):
    """
    Enhanced momentum trading strategy with position sizing and risk management
    """
    try:
        if not params or 'ticker' not in params:
            raise ValueError("Ticker symbol is required")
            
        ticker = params['ticker'].strip().upper()
        print(f"Executing momentum strategy for {ticker}...")
        
        # 获取历史数据
        data = data_fetcher.get_historical_data(ticker)
        
        if data is None or len(data) < 50:  # 确保有足够的历史数据
            raise ValueError(f"Insufficient data for {ticker}")
        
        # 计算技术指标
        data['MA20'] = data['Close'].rolling(window=20).mean()
        data['MA50'] = data['Close'].rolling(window=50).mean()
        data['RSI'] = calculate_rsi(data['Close'], periods=14)
        data['MACD'], data['Signal'], data['Hist'] = calculate_macd(data['Close'])
        
        # 获取最新数据
        latest_price = data['Close'].iloc[-1]
        latest_ma20 = data['MA20'].iloc[-1]
        latest_ma50 = data['MA50'].iloc[-1]
        latest_rsi = data['RSI'].iloc[-1]
        latest_macd = data['MACD'].iloc[-1]
        latest_signal = data['Signal'].iloc[-1]
        
        # 生成交易信号
        signal = generate_signal(latest_ma20, latest_ma50, latest_rsi, latest_macd, latest_signal)
        
        # 计算仓位大小和风险参数
        account_value = ib_connector.get_account_value()
        position_size = risk_manager.calculate_position_size(account_value, latest_price)
        stop_loss = risk_manager.calculate_stop_loss(latest_price)
        take_profit = risk_manager.calculate_take_profit(latest_price)
        
        # 执行交易
        if signal == "BUY":
            order = ib_connector.place_order(
                ticker=ticker,
                action="BUY",
                quantity=position_size,
                order_type="MKT",
                stop_loss=stop_loss,
                take_profit=take_profit
            )
            print(f"Placed BUY order for {ticker}: {order}")
            
        elif signal == "SELL":
            order = ib_connector.place_order(
                ticker=ticker,
                action="SELL",
                quantity=position_size,
                order_type="MKT",
                stop_loss=take_profit,  # 对于卖空，止损和止盈相反
                take_profit=stop_loss
            )
            print(f"Placed SELL order for {ticker}: {order}")
        
        # 打印策略结果
        print("\nStrategy Analysis:")
        print(f"Current Price: ${latest_price:.2f}")
        print(f"Position Size: {position_size:.0f} shares")
        print(f"Stop Loss: ${stop_loss:.2f}")
        print(f"Take Profit: ${take_profit:.2f}")
        print(f"Signal: {signal}")
        
    except Exception as e:
        print(f"Strategy Error: {str(e)}")
        raise

def calculate_rsi(prices, periods=14):
    """Calculate RSI technical indicator"""
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=periods).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=periods).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_macd(prices, fast=12, slow=26, signal=9):
    """Calculate MACD technical indicator"""
    exp1 = prices.ewm(span=fast, adjust=False).mean()
    exp2 = prices.ewm(span=slow, adjust=False).mean()
    macd = exp1 - exp2
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    histogram = macd - signal_line
    return macd, signal_line, histogram

def generate_signal(ma20, ma50, rsi, macd, signal_line):
    """Generate trading signal based on multiple indicators"""
    signal = "NEUTRAL"
    
    # 动量信号条件
    if (ma20 > ma50 and  # 金叉
        rsi < 70 and     # RSI不过热
        macd > signal_line):  # MACD金叉
        signal = "BUY"
    elif (ma20 < ma50 and  # 死叉
          rsi > 30 and     # RSI不过冷
          macd < signal_line):  # MACD死叉
        signal = "SELL"
        
    return signal