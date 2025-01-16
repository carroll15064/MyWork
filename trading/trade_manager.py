import threading
import time
from datetime import datetime, timedelta
import pandas as pd
import logging

class TradeManager:
    def __init__(self, ib_connector, risk_manager, data_fetcher):
        self.ib_connector = ib_connector
        self.risk_manager = risk_manager
        self.data_fetcher = data_fetcher
        self.active_trades = {}
        self.is_trading = False
        self.trading_thread = None
        self.check_interval = 60
        
        # 设置日志
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            filename='trading.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
    def start_auto_trading(self, strategy_name, strategy_func, params):
        """启动自动交易"""
        try:
            if self.is_trading:
                logging.warning("自动交易已启动")
                return False
                
            self.is_trading = True
            self.trading_thread = threading.Thread(
                target=self._trading_loop,
                args=(strategy_name, strategy_func, params)
            )
            self.trading_thread.daemon = True
            self.trading_thread.start()
            
            msg = f"Started auto trading for {strategy_name}"
            logging.info(msg)
            print(msg)
            return True
            
        except Exception as e:
            self.is_trading = False
            logging.error(f"Error starting auto trading: {str(e)}")
            print(f"Error starting auto trading: {str(e)}")
            return False
            
    def stop_auto_trading(self):
        """停止自动交易"""
        try:
            if not self.is_trading:
                return
                
            self.is_trading = False
            if self.trading_thread and self.trading_thread.is_alive():
                self.trading_thread.join(timeout=5.0)
            self._close_all_positions()
            
            msg = "停止自动交易并关闭所有持仓"
            logging.info(msg)
            print(msg)
            return True
            
        except Exception as e:
            logging.error(f"停止自动交易错误: {str(e)}")
            print(f"停止自动交易错误: {str(e)}")
            return False
            
    def _trading_loop(self, strategy_name, strategy_func, params):
        """交易主循环"""
        while self.is_trading:
            try:
                if not self._is_trading_hours():
                    time.sleep(60)
                    continue
                    
                # 执行策略
                result = strategy_func(
                    self.data_fetcher,
                    self.ib_connector,
                    self.risk_manager,
                    params
                )
                
                if result:
                    # 更新持仓
                    self._update_positions(strategy_name, result)
                    
                    # 检查止损止盈
                    self._check_exit_conditions()
                    
                # 等待下次检查
                time.sleep(self.check_interval)
                
            except Exception as e:
                logging.error(f"交易循环错误: {str(e)}")
                print(f"交易循环错误: {str(e)}")
                time.sleep(self.check_interval)
                
    def _is_trading_hours(self):
        """检查是否在交易时段"""
        now = datetime.now()
        
        # 如果是周末，不交易
        if now.weekday() in [5, 6]:
            return False
            
        # 美股交易时间 9:30 - 16:00 EST
        market_open = now.replace(hour=9, minute=30, second=0)
        market_close = now.replace(hour=16, minute=0, second=0)
        
        return market_open <= now <= market_close
        
    def _update_positions(self, strategy_name, strategy_result):
        """更新持仓信息"""
        if not strategy_result:
            return
            
        current_positions = self.ib_connector.get_positions()
        
        # 处理动量策略结果
        if strategy_name == "Momentum":
            ticker = strategy_result.get('ticker')
            signal = strategy_result.get('signal')
            
            if signal == "BUY" and ticker not in current_positions:
                self._execute_momentum_trade(ticker, "BUY", strategy_result)
            elif signal == "SELL" and ticker in current_positions:
                self._execute_momentum_trade(ticker, "SELL", strategy_result)
                
        # 处理统计套利策略结果
        elif strategy_name == "Statistical Arbitrage":
            pair = strategy_result.get('pair')
            signal = strategy_result.get('signal')
            
            if signal in ["OPEN_LONG_SHORT", "OPEN_SHORT_LONG"]:
                self._execute_pairs_trade(pair, signal, strategy_result)
                
    def _execute_momentum_trade(self, ticker, action, strategy_result):
        """执行动量策略交易"""
        try:
            # 首先验证是否能获取市场数据
            current_price = self.ib_connector.get_market_price(ticker)
            if not current_price:
                logging.error(f"无法获取 {ticker} 的市场价格，交易取消")
                return False
                
            account_value = self.ib_connector.get_account_value()
            
            # 添加详细日志
            logging.info(f"准备执行 {ticker} 的 {action} 交易")
            logging.info(f"当前价格: {current_price}")
            logging.info(f"账户价值: {account_value}")
            
            # 计算仓位大小
            position_size = self.risk_manager.calculate_position_size(
                account_value, current_price
            )
            
            # 执行交易
            order_result = self.ib_connector.place_order(
                ticker=ticker,
                action=action,
                quantity=position_size
            )
            
            if order_result:
                logging.info(f"成功执行 {ticker} {action} 订单，数量: {position_size}")
                # 记录交易
                self.active_trades[ticker] = {
                    'order': order_result,
                    'entry_price': current_price,
                    'strategy': 'Momentum'
                }
                return True
            else:
                logging.error(f"下单失败: {ticker}")
                return False
                
        except Exception as e:
            logging.error(f"执行动量交易时发生错误: {str(e)}")
            return False
            
    def _execute_pairs_trade(self, pair, signal, strategy_result):
        """执行配对交易"""
        try:
            ticker1, ticker2 = pair
            hedge_ratio = strategy_result.get('hedge_ratio')
            
            account_value = self.ib_connector.get_account_value()
            price1 = self.ib_connector.get_market_price(ticker1)
            price2 = self.ib_connector.get_market_price(ticker2)
            
            if not price1 or not price2:
                raise ValueError("无法获取市场价格")
                
            # 计算仓位大小
            position_value = account_value * self.risk_manager.max_position_size
            qty1 = int(position_value / (2 * price1))
            qty2 = int(qty1 * hedge_ratio)
            
            if signal == "OPEN_LONG_SHORT":
                order1 = self.ib_connector.place_order(ticker1, "BUY", qty1)
                order2 = self.ib_connector.place_order(ticker2, "SELL", qty2)
            else:  # OPEN_SHORT_LONG
                order1 = self.ib_connector.place_order(ticker1, "SELL", qty1)
                order2 = self.ib_connector.place_order(ticker2, "BUY", qty2)
                
            # 记录交易
            self.active_trades[f"{ticker1}-{ticker2}"] = {
                'orders': (order1, order2),
                'entry_prices': (price1, price2),
                'hedge_ratio': hedge_ratio,
                'strategy': 'Pairs'
            }
            
            logging.info(f"Executed pairs trade for {ticker1}-{ticker2}")
            
        except Exception as e:
            logging.error(f"Error executing pairs trade: {str(e)}")
            
    def _check_exit_conditions(self):
        """检查是否要平仓"""
        for trade_id, trade in list(self.active_trades.items()):
            try:
                if trade['strategy'] == 'Momentum':
                    self._check_momentum_exit(trade_id, trade)
                elif trade['strategy'] == 'Pairs':
                    self._check_pairs_exit(trade_id, trade)
                    
            except Exception as e:
                logging.error(f"Error checking exit conditions: {str(e)}")
                
    def _check_momentum_exit(self, ticker, trade):
        """检查动量策略平仓条件"""
        current_price = self.ib_connector.get_market_price(ticker)
        if not current_price:
            return
            
        # 检查止损止盈
        if current_price <= trade['stop_loss'] or current_price >= trade['take_profit']:
            self.ib_connector.close_position(ticker)
            del self.active_trades[ticker]
            
            msg = f"Closed position for {ticker} at {current_price}"
            logging.info(msg)
            print(msg)
            
    def _check_pairs_exit(self, pair_id, trade):
        """检查配对交易平仓条件"""
        ticker1, ticker2 = pair_id.split('-')
        price1 = self.ib_connector.get_market_price(ticker1)
        price2 = self.ib_connector.get_market_price(ticker2)
        
        if not price1 or not price2:
            return
            
        # 计算当前价差
        spread = price1 - trade['hedge_ratio'] * price2
        
        # 如果价差回归，平仓
        if abs(spread) < 0.5:  # 可以根据需要调整阈值
            self.ib_connector.close_position(ticker1)
            self.ib_connector.close_position(ticker2)
            del self.active_trades[pair_id]
            
            msg = f"Closed pairs position for {pair_id}"
            logging.info(msg)
            print(msg)
            
    def _close_all_positions(self):
        """关闭所有持仓"""
        for trade_id in list(self.active_trades.keys()):
            if '-' in trade_id:  # 配对交易
                ticker1, ticker2 = trade_id.split('-')
                self.ib_connector.close_position(ticker1)
                self.ib_connector.close_position(ticker2)
            else:  # 单个持仓
                self.ib_connector.close_position(trade_id)
                
        self.active_trades.clear() 