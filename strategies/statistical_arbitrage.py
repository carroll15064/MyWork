import pandas as pd
import numpy as np
from scipy import stats
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint
import warnings
warnings.filterwarnings('ignore')

def statistical_arbitrage_strategy(data_fetcher, ib_connector, risk_manager, params):
    """
    Enhanced statistical arbitrage strategy with improved pair selection
    """
    try:
        if not params or 'ticker1' not in params or 'ticker2' not in params:
            raise ValueError("Two ticker symbols are required for pairs trading")
            
        ticker1 = params['ticker1'].strip().upper()
        ticker2 = params['ticker2'].strip().upper()
        
        print(f"\nAnalyzing pair: {ticker1} - {ticker2}")
        
        # 获取历史数据
        data1 = data_fetcher.get_historical_data(ticker1)
        data2 = data_fetcher.get_historical_data(ticker2)
        
        # 确保两个时间序列对齐
        common_dates = data1.index.intersection(data2.index)
        if len(common_dates) < 252:  # 至少需要一年的数据
            raise ValueError("Insufficient historical data for pair analysis")
            
        prices1 = data1.loc[common_dates, 'Close']
        prices2 = data2.loc[common_dates, 'Close']
        
        # 标准化价格
        norm_prices1 = (prices1 - prices1.mean()) / prices1.std()
        norm_prices2 = (prices2 - prices2.mean()) / prices2.std()
        
        # 计算相关性
        correlation = norm_prices1.corr(norm_prices2)
        print(f"Price Correlation: {correlation:.4f}")
        
        if abs(correlation) < 0.7:
            raise ValueError(f"Insufficient price correlation ({correlation:.4f}) between {ticker1} and {ticker2}")
        
        # 计算协整关系
        coint_result = calculate_cointegration(prices1, prices2)
        if not coint_result['cointegrated']:
            suggest_pairs = suggest_alternative_pairs(ticker1, data_fetcher)
            raise ValueError(
                f"Pairs {ticker1} and {ticker2} are not cointegrated (p-value: {coint_result['p_value']:.4f})\n"
                f"Suggested alternative pairs for {ticker1}: {', '.join(suggest_pairs)}"
            )
            
        # 计算价差和z-score
        spread = calculate_spread(prices1, prices2, coint_result['hedge_ratio'])
        z_score = calculate_zscore(spread)
        
        # 生成交易信号
        current_zscore = z_score.iloc[-1]
        signal = generate_pairs_signal(current_zscore)
        
        if signal != "NEUTRAL":
            # 获取账户价值和计算仓位大小
            account_value = ib_connector.get_account_value()
            position_value = account_value * risk_manager.max_position_size
            
            # 计算每只股票的具体仓位
            price1 = prices1.iloc[-1]
            price2 = prices2.iloc[-1]
            qty1 = int(position_value / (2 * price1))
            qty2 = int(qty1 * coint_result['hedge_ratio'])
            
            # 执行交易
            if signal == "OPEN_LONG_SHORT":
                order1 = ib_connector.place_order(ticker1, "BUY", qty1, "MKT")
                order2 = ib_connector.place_order(ticker2, "SELL", qty2, "MKT")
                print(f"Opened long {ticker1} ({qty1} shares) / short {ticker2} ({qty2} shares)")
                
            elif signal == "OPEN_SHORT_LONG":
                order1 = ib_connector.place_order(ticker1, "SELL", qty1, "MKT")
                order2 = ib_connector.place_order(ticker2, "BUY", qty2, "MKT")
                print(f"Opened short {ticker1} ({qty1} shares) / long {ticker2} ({qty2} shares)")
        
        # 打印详细分析结果
        print("\nPairs Trading Analysis:")
        print(f"Pair: {ticker1} - {ticker2}")
        print(f"Correlation: {correlation:.4f}")
        print(f"Cointegration p-value: {coint_result['p_value']:.4f}")
        print(f"Hedge Ratio: {coint_result['hedge_ratio']:.4f}")
        print(f"Current Z-Score: {current_zscore:.2f}")
        print(f"Signal: {signal}")
        
        # 返回分析结果
        return {
            'pair': (ticker1, ticker2),
            'correlation': correlation,
            'hedge_ratio': coint_result['hedge_ratio'],
            'zscore': current_zscore,
            'signal': signal
        }
        
    except Exception as e:
        print(f"Strategy Error: {str(e)}")
        raise

def calculate_cointegration(prices1, prices2):
    """Calculate cointegration relationship between two price series"""
    try:
        # 添加常数项进行回归
        X = sm.add_constant(prices1)
        model = sm.OLS(prices2, X).fit()
        
        # 使用模型参数中的系数作为对冲比率
        hedge_ratio = model.params.iloc[1]  # 使用 iloc 避免废弃警告
        
        # 进行协整检验
        _, pvalue, _ = coint(prices1, prices2)
        cointegrated = pvalue < 0.05
        
        return {
            'hedge_ratio': hedge_ratio,
            'cointegrated': cointegrated,
            'p_value': pvalue
        }
    except Exception as e:
        raise ValueError(f"Error in cointegration calculation: {str(e)}")

def calculate_spread(prices1, prices2, hedge_ratio):
    """Calculate the spread between two price series"""
    return prices1 - hedge_ratio * prices2

def calculate_zscore(spread, window=20):
    """Calculate rolling z-score of spread"""
    mean = spread.rolling(window=window).mean()
    std = spread.rolling(window=window).std()
    return (spread - mean) / std

def generate_pairs_signal(z_score, entry_threshold=2.0, exit_threshold=0.5):
    """Generate trading signal based on z-score"""
    if z_score > entry_threshold:
        return "OPEN_SHORT_LONG"
    elif z_score < -entry_threshold:
        return "OPEN_LONG_SHORT"
    elif abs(z_score) < exit_threshold:
        return "CLOSE_POSITION"
    return "NEUTRAL"

def suggest_alternative_pairs(ticker, data_fetcher, top_n=5):
    """建议替代的配对交易股票"""
    # 这里可以根据行业分类或其他标准推荐相关股票
    # 示例配对（实际应用中应该基于更复杂的分析）
    sector_pairs = {
        'AMD': ['NVDA', 'INTC', 'MU', 'TSM', 'QCOM'],
        'TSLA': ['F', 'GM', 'TM', 'RIVN', 'NIO'],
        'AAPL': ['MSFT', 'GOOGL', 'META', 'AMZN', 'NFLX'],
        # 可以添加更多股票对
    }
    
    return sector_pairs.get(ticker, ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META'])[:top_n] 