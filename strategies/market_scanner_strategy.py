import logging
from datetime import datetime, timedelta

from scanners.stock_scanner import StockScanner


def market_scanner_strategy(data_fetcher, ib_connector, risk_manager, params):
    """
    基于市场扫描的自动交易策略
    """
    try:
        # 获取策略参数
        min_momentum_score = params.get('min_momentum_score', 0.6)
        min_volume_score = params.get('min_volume_score', 0.5)
        max_volatility = params.get('max_volatility', 0.4)
        max_positions = params.get('max_positions', 5)
        
        # 创建扫描器
        scanner = StockScanner(data_fetcher)
        
        # 扫描市场
        candidates = scanner.scan_market(
            min_momentum_score=min_momentum_score,
            min_volume_score=min_volume_score,
            max_volatility=max_volatility
        )
        
        if not candidates:
            print("No suitable candidates found")
            return None
            
        # 获取当前持仓
        current_positions = ib_connector.get_positions()
        
        # 获取账户价值
        account_value = ib_connector.get_account_value()
        
        # 处理每个候选股票
        for candidate in candidates[:max_positions]:  # 限制最大持仓数量
            symbol = candidate['symbol']
            
            # 如果已经持有该股票，跳过
            if symbol in current_positions:
                continue
                
            # 如果持仓数量已达到最大值，跳过
            if len(current_positions) >= max_positions:
                break
                
            # 获取当前市场价格
            current_price = ib_connector.get_market_price(symbol)
            if not current_price:
                continue
                
            # 计算仓位大小
            position_size = risk_manager.calculate_position_size(
                account_value, current_price
            )
            
            # 计算止损和止盈价格
            stop_loss = risk_manager.calculate_stop_loss(current_price)
            take_profit = risk_manager.calculate_take_profit(current_price)
            
            # 执行买入订单
            order = ib_connector.place_order(
                ticker=symbol,
                action="BUY",
                quantity=position_size,
                stop_loss=stop_loss,
                take_profit=take_profit
            )
            
            if order:
                print(f"Placed order for {symbol}")
                logging.info(f"Placed order for {symbol}: {position_size} shares at {current_price}")
                
        return {
            'candidates': candidates,
            'positions': current_positions
        }
        
    except Exception as e:
        logging.error(f"Error in market scanner strategy: {str(e)}")
        raise 