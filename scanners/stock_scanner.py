import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import logging

class StockScanner:
    def __init__(self, data_fetcher):
        self.data_fetcher = data_fetcher
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            filename='scanner.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
    def calculate_rsi(self, prices, periods=14):
        """计算RSI指标"""
        deltas = np.diff(prices)
        seed = deltas[:periods+1]
        up = seed[seed >= 0].sum()/periods
        down = -seed[seed < 0].sum()/periods
        rs = up/down
        rsi = np.zeros_like(prices)
        rsi[:periods] = 100. - 100./(1. + rs)

        for i in range(periods, len(prices)):
            delta = deltas[i - 1]
            if delta > 0:
                upval = delta
                downval = 0.
            else:
                upval = 0.
                downval = -delta

            up = (up*(periods-1) + upval)/periods
            down = (down*(periods-1) + downval)/periods
            rs = up/down
            rsi[i] = 100. - 100./(1. + rs)

        return pd.Series(rsi, index=prices.index)
        
    def calculate_macd(self, prices, fast=12, slow=26, signal=9):
        """计算MACD指标"""
        exp1 = prices.ewm(span=fast, adjust=False).mean()
        exp2 = prices.ewm(span=slow, adjust=False).mean()
        macd = exp1 - exp2
        signal_line = macd.ewm(span=signal, adjust=False).mean()
        return macd, signal_line
        
    def calculate_sma(self, prices, period):
        """计算简单移动平均"""
        return prices.rolling(window=period).mean()
        
    def get_sp500_symbols(self):
        """获取美股股票列表"""
        try:
            # 使用 DataFetcher 获取股票列表
            symbols = self.data_fetcher.get_us_stock_list()
            
            # 过滤掉任何无效的符号
            valid_symbols = []
            for symbol in symbols:
                # 确保符号是有效的
                if isinstance(symbol, str) and symbol.replace('.', '').isalnum():
                    valid_symbols.append(symbol.strip())
                    
            if not valid_symbols:
                raise ValueError("No valid symbols found")
                
            logging.info(f"Successfully loaded {len(valid_symbols)} symbols")
            return valid_symbols
            
        except Exception as e:
            logging.error(f"Error in get_sp500_symbols: {str(e)}")
            # 返回一个最小的备用列表
            return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
            
    def analyze_stock(self, symbol):
        """分析单个股票"""
        try:
            # 清理股票代码
            clean_symbol = symbol.split('.')[-1] if '.' in symbol else symbol
            
            # 跳过权证和特殊证券
            if (clean_symbol.endswith('W') or clean_symbol.endswith('R') or 
                clean_symbol.endswith('U') or clean_symbol.endswith('S')):
                return None
                
            # 获取历史数据
            data = self.data_fetcher.get_historical_data(clean_symbol)
            if data is None or len(data) < 50:
                return None
                
            # 计算技术指标
            close_prices = data['Close']
            volume = data['Volume']
            
            # 计算动量指标
            rsi = self.calculate_rsi(close_prices)
            macd, signal = self.calculate_macd(close_prices)
            
            # 计算趋势指标
            sma20 = self.calculate_sma(close_prices, 20)
            sma50 = self.calculate_sma(close_prices, 50)
            
            # 计算成交量指标
            volume_sma20 = self.calculate_sma(volume, 20)
            
            # 获取最新数据
            current_price = close_prices.iloc[-1]
            current_volume = volume.iloc[-1]
            current_rsi = rsi.iloc[-1]
            current_macd = macd.iloc[-1]
            current_signal = signal.iloc[-1]
            current_sma20 = sma20.iloc[-1]
            current_sma50 = sma50.iloc[-1]
            
            # 计算波动率
            returns = np.log(close_prices / close_prices.shift(1))
            volatility = returns.std() * np.sqrt(252)
            
            # 计算动量得分
            momentum_score = self.calculate_momentum_score(
                current_price, current_sma20, current_sma50,
                current_rsi, current_macd, current_signal
            )
            
            # 计算成交量得分
            volume_score = self.calculate_volume_score(
                current_volume, volume_sma20.iloc[-1]
            )
            
            # 返回分析结果
            return {
                'symbol': symbol,
                'price': current_price,
                'momentum_score': momentum_score,
                'volume_score': volume_score,
                'volatility': volatility,
                'rsi': current_rsi,
                'above_sma20': current_price > current_sma20,
                'above_sma50': current_price > current_sma50,
                'macd_signal': current_macd > current_signal
            }
            
        except Exception as e:
            logging.error(f"Error analyzing {symbol}: {str(e)}")
            return None
            
    def calculate_momentum_score(self, price, sma20, sma50, rsi, macd, signal):
        """计算动量得分"""
        score = 0
        
        # 价格趋势
        if price > sma20:
            score += 1
        if price > sma50:
            score += 1
        if sma20 > sma50:
            score += 1
            
        # RSI
        if 40 < rsi < 70:  # 健康��RSI范围
            score += 1
        elif 30 < rsi <= 40:  # 可能超卖
            score += 0.5
            
        # MACD
        if macd > signal:
            score += 1
            
        return score / 5  # 归一化到0-1范围
        
    def calculate_volume_score(self, current_volume, volume_sma20):
        """计算成交量得分"""
        if volume_sma20 == 0:
            return 0
        volume_ratio = current_volume / volume_sma20
        if volume_ratio > 2:
            return 1
        elif volume_ratio > 1.5:
            return 0.75
        elif volume_ratio > 1:
            return 0.5
        return 0
        
    def scan_market(self, min_momentum_score=0.6, min_volume_score=0.5, max_volatility=0.4):
        """扫描市场寻找优质股票"""
        try:
            symbols = self.get_sp500_symbols()
            results = []
            
            print(f"Scanning {len(symbols)} stocks...")
            
            # 使用线程池并行处理
            with ThreadPoolExecutor(max_workers=10) as executor:
                analyses = list(executor.map(self.analyze_stock, symbols))
                
            # 过滤和排序结果
            for analysis in analyses:
                if analysis is None:
                    continue
                    
                if (analysis['momentum_score'] >= min_momentum_score and
                    analysis['volume_score'] >= min_volume_score and
                    analysis['volatility'] <= max_volatility):
                    results.append(analysis)
                    
            # 按动量得分排序
            results.sort(key=lambda x: x['momentum_score'], reverse=True)
            
            return results
            
        except Exception as e:
            logging.error(f"Error in market scan: {str(e)}")
            return [] 