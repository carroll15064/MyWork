import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
import time
import logging
from requests.exceptions import RequestException
import numpy as np
import yfinance as yf
import requests.exceptions
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from ib_insync import Stock, util
import random
from bs4 import BeautifulSoup

class DataFetcher:
    def __init__(self):
        """初始化数据获取器"""
        self.setup_logging()
        self.cache = {}  # 数据缓存
        self.cache_timeout = 300  # 缓存超时时间（秒）
        self.last_request_time = 0
        self.request_delay = 1  # seconds
        self.setup_session()
        self.ib = None  # IB connection will be set externally
        # Finnhub API key
        self.finnhub_api_key = "cn7jj89r01qjr0uqb1s0cn7jj89r01qjr0uqb1sg"  # Free API key
        
    def setup_logging(self):
        """设置日志"""
        # Create logs directory if it doesn't exist
        log_dir = 'logs'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        # Create a file handler
        log_file = os.path.join(log_dir, 'data_fetcher.log')
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create formatters and add it to the handlers
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        file_handler.setFormatter(detailed_formatter)
        console_handler.setFormatter(simple_formatter)
        
        # Get the root logger
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        
        # Remove any existing handlers
        logger.handlers = []
        
        # Add the handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
    def setup_session(self):
        """设置请求会话"""
        self.session = requests.Session()
        
        # 设置重试策略
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        
        # 设置适配器
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
    def set_ib_connection(self, ib):
        """Set the IB connection for data fetching"""
        self.ib = ib
        
    def get_historical_data(self, ticker, period='1y', interval='1d'):
        """
        获取历史数据，自动尝试多个数据源
        """
        ticker = ticker.upper().strip()
        cache_key = f"{ticker}_{period}_{interval}"
        
        # 检查缓存
        if cache_key in self.cache:
            cache_time, data = self.cache[cache_key]
            if time.time() - cache_time < self.cache_timeout:
                return data
                
        # Try Finnhub first (primary source)
        data = self._get_finnhub_data(ticker, period, interval)
        if data is not None and not data.empty:
            # 统一数据格式
            data = self._standardize_data(data)
            # 更新缓存
            self.cache[cache_key] = (time.time(), data)
            logging.info(f"Successfully fetched data from Finnhub for {ticker}")
            return data
            
        # Try IB as backup
        data = self._get_ib_data(ticker, period, interval)
        if data is not None and not data.empty:
            # 统一数据格式
            data = self._standardize_data(data)
            # 更新缓存
            self.cache[cache_key] = (time.time(), data)
            logging.info(f"Successfully fetched data from IB for {ticker}")
            return data
            
        # Try yfinance as another backup
        data = self._get_yfinance_data(ticker, period, interval)
        if data is not None and not data.empty:
            # 统一数据格式
            data = self._standardize_data(data)
            # 更新缓存
            self.cache[cache_key] = (time.time(), data)
            logging.info(f"Successfully fetched data from yfinance for {ticker}")
            return data
            
        # 如果所有数据源都失败
        error_msg = f"Failed to fetch data for {ticker} from all sources"
        logging.error(error_msg)
        raise Exception(error_msg)
        
    def _standardize_data(self, data):
        """统一数据格式"""
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        
        # 确保所有必需的列都存在
        for col in required_columns:
            if col not in data.columns:
                raise ValueError(f"Missing required column: {col}")
                
        # 确保索引是日期时间类型
        if not isinstance(data.index, pd.DatetimeIndex):
            data.index = pd.to_datetime(data.index)
            
        # 按日期排序
        data = data.sort_index()
        
        # 处理缺失值
        data = data.fillna(method='ffill')
        
        return data
        
    def _get_finnhub_data(self, ticker, period='1y', interval='1d'):
        """从Finnhub获取数据"""
        try:
            logging.info(f"Attempting to fetch data from Finnhub for {ticker}")
            
            # Add delay between requests
            current_time = time.time()
            if current_time - self.last_request_time < self.request_delay:
                time.sleep(self.request_delay)
            self.last_request_time = current_time
            
            # Calculate timestamp range
            if period == '1y':
                start_date = datetime.now() - timedelta(days=365)
            elif period == '6m':
                start_date = datetime.now() - timedelta(days=180)
            else:
                start_date = datetime.now() - timedelta(days=30)
                
            end_date = datetime.now()
            
            # 使用股票代码格式化
            formatted_ticker = ticker.upper()
            if not formatted_ticker.endswith('.US'):
                formatted_ticker = f"{formatted_ticker}.US"
            
            url = "https://finnhub.io/api/v1/stock/candle"
            params = {
                "symbol": formatted_ticker,
                "resolution": "D",
                "from": int(start_date.timestamp()),
                "to": int(end_date.timestamp()),
                "token": self.finnhub_api_key
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'X-Finnhub-Token': self.finnhub_api_key
            }
            
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    response = self.session.get(url, params=params, headers=headers, timeout=15)
                    response.raise_for_status()  # 抛出HTTP错误
                    
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('s') == 'ok' and len(data.get('t', [])) > 0:
                            df = pd.DataFrame({
                                'Open': data['o'],
                                'High': data['h'],
                                'Low': data['l'],
                                'Close': data['c'],
                                'Volume': data['v']
                            }, index=pd.to_datetime(data['t'], unit='s'))
                            df = df.sort_index()
                            logging.info(f"Successfully fetched {len(df)} data points from Finnhub for {ticker}")
                            return df
                        else:
                            logging.warning(f"Finnhub returned no data for {ticker}: {data.get('s')}")
                    elif response.status_code == 429:
                        logging.warning(f"Rate limit exceeded for Finnhub API. Waiting 65 seconds...")
                        time.sleep(65)
                        continue
                        
                except requests.exceptions.RequestException as e:
                    logging.error(f"Request error on attempt {attempt + 1}: {str(e)}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                        
            logging.warning(f"{ticker}: Failed to fetch data from Finnhub after {max_retries} attempts")
            return None
            
        except Exception as e:
            logging.error(f"Error fetching Finnhub data for {ticker}: {str(e)}")
            return None
            
    def _get_ib_data(self, ticker, period='1y', interval='1d'):
        """从Interactive Brokers获取数据"""
        try:
            logging.info(f"Attempting to fetch data from IB for {ticker}")
            
            if not self.ib or not self.ib.isConnected():
                logging.warning("IB not connected or connection lost")
                return None
                
            # 添加重连逻辑
            max_reconnect_attempts = 3
            for reconnect_attempt in range(max_reconnect_attempts):
                try:
                    if not self.ib.isConnected():
                        logging.info(f"Reconnection attempt {reconnect_attempt + 1}")
                        self.ib.disconnect()
                        time.sleep(2)
                        self.ib.connect('127.0.0.1', 7497, clientId=random.randint(1000, 9999))
                        time.sleep(3)  # 等待连接建立
                        
                    if self.ib.isConnected():
                        break
                except Exception as e:
                    logging.error(f"Reconnection attempt {reconnect_attempt + 1} failed: {str(e)}")
                    if reconnect_attempt < max_reconnect_attempts - 1:
                        time.sleep(5)
                    continue
                    
            if not self.ib.isConnected():
                logging.error("Failed to establish IB connection after multiple attempts")
                return None
                
            # 创建合约
            contract = Stock(ticker, 'SMART', 'USD')
            logging.debug(f"Created contract: {contract}")
            
            # 设置时间范围
            if period == '1y':
                duration = '1 Y'
            elif period == '6m':
                duration = '6 M'
            else:
                duration = '1 M'
                
            max_data_retries = 3
            for attempt in range(max_data_retries):
                try:
                    logging.debug(f"Data fetch attempt {attempt + 1} for {ticker}")
                    
                    # 请求历史数据
                    bars = self.ib.reqHistoricalData(
                        contract,
                        endDateTime='',
                        durationStr=duration,
                        barSizeSetting='1 day',
                        whatToShow='TRADES',
                        useRTH=True,
                        formatDate=1,
                        timeout=45  # 增加超时时间
                    )
                    
                    if bars and len(bars) > 0:
                        df = util.df(bars)
                        if not df.empty:
                            df.set_index('date', inplace=True)
                            logging.info(f"Successfully fetched {len(df)} data points from IB for {ticker}")
                            return df
                            
                    time.sleep(3)  # 重试之间的延迟
                    
                except Exception as e:
                    logging.error(f"Data fetch attempt {attempt + 1} failed: {str(e)}")
                    if attempt < max_data_retries - 1:
                        time.sleep(5)
                    continue
                    
            logging.warning(f"{ticker}: Failed to fetch data from IB after {max_data_retries} attempts")
            return None
            
        except Exception as e:
            logging.error(f"Error in IB data fetching for {ticker}: {str(e)}")
            return None
            
    def _get_yfinance_data(self, ticker, period='1y', interval='1d'):
        """从Yahoo Finance获取数据"""
        try:
            logging.info(f"Attempting to fetch data from yfinance for {ticker}")
            
            # Add delay between requests
            current_time = time.time()
            if current_time - self.last_request_time < self.request_delay:
                time.sleep(self.request_delay)
            self.last_request_time = current_time
            
            # 创建自定义session
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
            })
            
            # 直接使用Yahoo Finance API
            url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}"
            params = {
                "range": "1y" if period == "1y" else "6mo" if period == "6m" else "1mo",
                "interval": "1d",
                "includePrePost": "false",
                "events": "div,splits"
            }
            
            response = session.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                result = data['chart']['result'][0]
                timestamps = result['timestamp']
                quote = result['indicators']['quote'][0]
                
                df = pd.DataFrame({
                    'Open': quote.get('open', []),
                    'High': quote.get('high', []),
                    'Low': quote.get('low', []),
                    'Close': quote.get('close', []),
                    'Volume': quote.get('volume', [])
                }, index=pd.to_datetime(timestamps, unit='s'))
                
                # 清理数据
                df = df.dropna()
                
                if not df.empty:
                    logging.info(f"Successfully fetched {len(df)} data points from Yahoo Finance for {ticker}")
                    return df
                    
            logging.warning(f"{ticker}: No data found from Yahoo Finance")
            return None
            
        except Exception as e:
            logging.error(f"Error fetching Yahoo Finance data for {ticker}: {str(e)}")
            return None
            
    def get_data(self, ticker, period='1y', interval='1d'):
        """获取股票数据，尝试多个数据源"""
        try:
            # Try Finnhub first (primary source)
            data = self._get_finnhub_data(ticker, period, interval)
            if data is not None and not data.empty:
                return data
                
            # Try IB as backup
            data = self._get_ib_data(ticker, period, interval)
            if data is not None and not data.empty:
                return data
                
            # Try yfinance as another backup
            data = self._get_yfinance_data(ticker, period, interval)
            if data is not None and not data.empty:
                return data
                
            raise Exception("Failed to fetch data from all sources")
            
        except Exception as e:
            logging.error(f"Failed to fetch data for {ticker} from all sources: {str(e)}")
            return None
            
    def get_real_time_price(self, ticker):
        """获取实时价格"""
        try:
            # 尝试从yfinance获取实时价格
            stock = yf.Ticker(ticker)
            price = stock.info.get('regularMarketPrice')
            
            if price:
                return float(price)
                
            # 如果yfinance失败，尝试其他数据源
            data = self.get_historical_data(ticker, period='1d', interval='1m')
            if not data.empty:
                return float(data['Close'].iloc[-1])
                
            raise Exception("Could not get real-time price")
            
        except Exception as e:
            logging.error(f"Error getting real-time price for {ticker}: {str(e)}")
            raise
            
    def get_multiple_historical_data(self, tickers, period='1y', interval='1d'):
        """并行获取多个股票的历史数据"""
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_ticker = {
                executor.submit(self.get_historical_data, ticker, period, interval): ticker
                for ticker in tickers
            }
            
            results = {}
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    data = future.result()
                    results[ticker] = data
                except Exception as e:
                    logging.error(f"Error fetching data for {ticker}: {str(e)}")
                    results[ticker] = None
                    
            return results