from ib_insync import *
import time
import logging
import asyncio
import nest_asyncio
from threading import Thread
import numpy as np

# 允许在Jupyter中嵌套事件循环
nest_asyncio.apply()

class IBConnector:
    def __init__(self, host='127.0.0.1', port=7497, client_id=None):
        self.ib = IB()
        self.host = host
        self.port = port
        self.client_id = client_id if client_id else np.random.randint(1000, 9999)
        self.connected = False
        self.account = None
        self._event_loop = None
        self._thread = None
        
        # 设置日志
        logging.basicConfig(
            filename='ib_trades.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
    def _start_event_loop(self):
        """在新线程中启动事件循环"""
        self._event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._event_loop)
        self._event_loop.run_forever()
        
    def _ensure_event_loop(self):
        """确保事件循环正在运行"""
        if self._thread is None:
            self._thread = Thread(target=self._start_event_loop, daemon=True)
            self._thread.start()
            time.sleep(1)  # 等待事件循环启动
            
    def _run_async(self, coro):
        """在事件循环中运行协程"""
        self._ensure_event_loop()
        future = asyncio.run_coroutine_threadsafe(coro, self._event_loop)
        return future.result()
        
    def connect(self):
        """连接到 Interactive Brokers TWS"""
        try:
            if not self.connected:
                self._ensure_event_loop()
                self._run_async(self.ib.connectAsync(self.host, self.port, clientId=self.client_id))
                self.connected = True
                time.sleep(1)  # 等待连接建立
                
                # 获取账户信息
                accounts = self.ib.wrapper.accounts
                if accounts:
                    self.account = accounts[0]
                    print(f"成功连接到 IB (Account: {self.account})")
                    return True
                else:
                    raise Exception("No accounts found")
                    
        except Exception as e:
            print(f"Failed to connect to IB: {str(e)}")
            self.connected = False
            return False
            
    def get_account_value(self):
        """获取账户价值"""
        try:
            if not self.connected:
                raise Exception("Not connected to IB")
                
            values = self._run_async(self.ib.reqAccountSummaryAsync())
            for v in values:
                if v.tag == 'NetLiquidation':
                    return float(v.value)
            return 0
        except Exception as e:
            logging.error(f"Error getting account value: {str(e)}")
            return 0
            
    def place_order(self, ticker, action, quantity, order_type='MKT', 
                   limit_price=None, stop_loss=None, take_profit=None):
        """下单函数 - 确保只下整数股数的订单"""
        try:
            if not self.connected:
                raise Exception("Not connected to IB")
                
            # 将数量转换为整数
            quantity = int(quantity)
            if quantity <= 0:
                raise ValueError("Order quantity must be positive")
                
            # 创建合约
            contract = Stock(ticker, 'SMART', 'USD')
            
            # 创建主订单
            if order_type == 'MKT':
                order = MarketOrder(action, quantity)
            elif order_type == 'LMT':
                if limit_price is None:
                    raise ValueError("Limit price is required for LMT orders")
                order = LimitOrder(action, quantity, limit_price)
            else:
                raise ValueError(f"Unsupported order type: {order_type}")
                
            # 下主订单
            trade = self._run_async(self.ib.placeOrderAsync(contract, order))
            
            # 如果设置了止损
            if stop_loss is not None:
                stop_action = 'SELL' if action == 'BUY' else 'BUY'
                stop_order = StopOrder(stop_action, quantity, stop_loss)
                self._run_async(self.ib.placeOrderAsync(contract, stop_order))
                
            # 如果设置了止盈
            if take_profit is not None:
                profit_action = 'SELL' if action == 'BUY' else 'BUY'
                profit_order = LimitOrder(profit_action, quantity, take_profit)
                self._run_async(self.ib.placeOrderAsync(contract, profit_order))
                
            return True
            
        except Exception as e:
            logging.error(f"Error placing order: {str(e)}")
            return False
            
    def get_positions(self):
        """获取当前持仓"""
        try:
            if not self.connected:
                raise Exception("Not connected to IB")
                
            positions = self._run_async(self.ib.reqPositionsAsync())
            return positions
        except Exception as e:
            logging.error(f"Error getting positions: {str(e)}")
            return []
            
    def close_position(self, ticker):
        """关闭指定股票的持仓"""
        try:
            if not self.connected:
                raise Exception("Not connected to IB")
                
            positions = self.get_positions()
            for position in positions:
                if position.contract.symbol == ticker:
                    quantity = abs(position.position)
                    action = 'SELL' if position.position > 0 else 'BUY'
                    return self.place_order(ticker, action, quantity)
            return True  # 如果没有持仓，也返回成功
        except Exception as e:
            logging.error(f"Error closing position: {str(e)}")
            return False
            
    def close_all_positions(self):
        """关闭所有持仓"""
        try:
            if not self.connected:
                raise Exception("Not connected to IB")
                
            positions = self.get_positions()
            success = True
            for position in positions:
                if not self.close_position(position.contract.symbol):
                    success = False
            return success
        except Exception as e:
            logging.error(f"Error closing all positions: {str(e)}")
            return False
            
    def disconnect(self):
        """断开连接"""
        try:
            if self.connected:
                self.ib.disconnect()
                self.connected = False
                
            if self._event_loop is not None:
                self._event_loop.call_soon_threadsafe(self._event_loop.stop)
                self._thread.join(timeout=5)
                self._event_loop = None
                self._thread = None
                
        except Exception as e:
            logging.error(f"Error disconnecting: {str(e)}")
            
    def get_market_price(self, ticker):
        """获取市场实时价格"""
        try:
            if not self.connected:
                raise Exception("Not connected to IB")
                
            contract = Stock(ticker, 'SMART', 'USD')
            
            # 请求市场数据
            ticker_data = self._run_async(self.ib.reqMktDataAsync(contract))
            self.ib.sleep(2)  # 等待数据返回
            
            if ticker_data.last:
                return ticker_data.last
            elif ticker_data.close:
                return ticker_data.close
                
            raise Exception(f"无法获取市场价格 {ticker}")
            
        except Exception as e:
            logging.error(f"错误获取市场价格: {str(e)}")
            return None
            
    def get_account_value(self):
        """获取账户价值"""
        try:
            if not self.connected:
                raise Exception("无法连接到IB")
                
            # 请求账户摘要
            values = self._run_async(self.ib.reqAccountSummaryAsync())
            
            # 查找 NetLiquidation 值
            for v in values:
                if v.tag == 'NetLiquidation':
                    return float(v.value)
                    
            raise Exception("无法找到账户价值")
            
        except Exception as e:
            logging.error(f"错误获取账户价值: {str(e)}")
            return 0
            
    def place_order(self, ticker, action, quantity, order_type='MKT', 
                   limit_price=None, stop_loss=None, take_profit=None):
        """
        下单函数 - 确保只下整数股数的订单
        """
        try:
            if not self.connected:
                raise Exception("无法连接到IB")
                
            # 将股数转换为整数
            quantity = int(quantity)  # 向下取整
            if quantity <= 0:
                raise ValueError("Order quantity must be positive")
                
            # 创建合约对象
            contract = Stock(ticker, 'SMART', 'USD')
            self.ib.qualifyContracts(contract)
            
            # 创建主订单
            if order_type == 'MKT':
                main_order = MarketOrder(action, quantity)
            elif order_type == 'LMT':
                if limit_price is None:
                    raise ValueError("Limit price required for LMT order")
                main_order = LimitOrder(action, quantity, limit_price)
            else:
                raise ValueError(f"Unsupported order type: {order_type}")
                
            # 添加订单属性
            main_order.account = self.account
            main_order.transmit = True
            
            # 记录订单信息
            logging.info(f"尝试下 {action} 订单 {quantity} 股 {ticker}")
            print(f"尝试下 {action} 订单 {quantity} 股 {ticker}")
            
            # 发送主订单
            trade = self._run_async(self.ib.placeOrderAsync(contract, main_order))
            
            # 等待订单状态更新
            for i in range(5):
                self.ib.sleep(1)
                if trade.orderStatus.status not in ['Submitted', 'PendingSubmit']:
                    break
                    
            # 检查订单状态
            if trade.orderStatus.status == 'Cancelled':
                error_msg = f"订单取消: {trade.orderStatus.whyHeld}"
                logging.error(error_msg)
                print(error_msg)
                return None
                
            if trade.orderStatus.status == 'Filled':
                # 添加止损单
                if stop_loss is not None:
                    stop_action = 'SELL' if action == 'BUY' else 'BUY'
                    stop_order = StopOrder(stop_action, quantity, stop_loss)
                    stop_order.account = self.account
                    stop_order.transmit = True
                    self._run_async(self.ib.placeOrderAsync(contract, stop_order))
                    logging.info(f"Placed stop loss order at {stop_loss}")
                    
                # 添加止盈单
                if take_profit is not None:
                    profit_action = 'SELL' if action == 'BUY' else 'BUY'
                    profit_order = LimitOrder(profit_action, quantity, take_profit)
                    profit_order.account = self.account
                    profit_order.transmit = True
                    self._run_async(self.ib.placeOrderAsync(contract, profit_order))
                    logging.info(f"Placed take profit order at {take_profit}")
                    
            return trade
            
        except Exception as e:
            error_msg = f"Error placing order: {str(e)}"
            logging.error(error_msg)
            print(error_msg)
            raise
            
    def close_position(self, ticker):
        """关闭指定股票的持仓"""
        try:
            positions = self.get_positions()
            if ticker in positions:
                quantity = abs(positions[ticker])
                action = 'SELL' if positions[ticker] > 0 else 'BUY'
                return self.place_order(ticker, action, quantity)
            return None
            
        except Exception as e:
            logging.error(f"错误关闭持仓 {ticker}: {str(e)}")
            print(f"错误关闭持仓 {ticker}: {str(e)}")
            
    def get_positions(self):
        """获取当前持仓"""
        try:
            if not self.connected:
                raise Exception("链接未建立")
                
            positions = {}
            for p in self._run_async(self.ib.reqPositionsAsync()):
                positions[p.contract.symbol] = p.position
                
            return positions
            
        except Exception as e:
            logging.error(f"错误获取持仓: {str(e)}")
            print(f"错误获取持仓: {str(e)}")
            return {}
            
    def get_market_price(self, ticker):
        """获取市场实时价格"""
        try:
            if not self.connected:
                raise Exception("链接未建立")
                
            contract = Stock(ticker, 'SMART', 'USD')
            self.ib.qualifyContracts(contract)
            
            # 请求市场数据
            ticker_data = self._run_async(self.ib.reqMktDataAsync(contract))
            self.ib.sleep(2)  # 等待数据返回
            
            if ticker_data.last:
                return ticker_data.last
            elif ticker_data.close:
                return ticker_data.close
                
            raise Exception(f"无法获取市场价格 {ticker}")
            
        except Exception as e:
            logging.error(f"错误获取市场价格: {str(e)}")
            print(f"错误获取市场价格: {str(e)}")
            return None
            
    def disconnect(self):
        """断开与 IB 的连接"""
        if self.connected:
            self.ib.disconnect()
            self.connected = False
            
    def test_connection(self):
        """测试连接和数据访问"""
        try:
            # 测试市场数据访问
            test_symbol = 'AMD'
            price = self.get_market_price(test_symbol)
            if price:
                logging.info(f"成功获取 {test_symbol} 价格: {price}")
                return True
            else:
                logging.error(f"无法获取 {test_symbol} 的价格数据")
                return False
        except Exception as e:
            logging.error(f"连接测试失败: {str(e)}")
            return False