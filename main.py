from data.data_fetcher import DataFetcher
from broker.ib_connector import IBConnector
from risk_management.risk_manager import RiskManager
from gui.trading_interface import TradingGUI
from strategies.market_scanner_strategy import market_scanner_strategy
from trading.trade_manager import TradeManager
from strategies.momentum import momentum_strategy
from strategies.mean_reversion import mean_reversion_strategy
from strategies.statistical_arbitrage import statistical_arbitrage_strategy

def main():
    # Initialize components
    data_fetcher = DataFetcher()
    ib_connector = IBConnector()
    

    
    # Connect to Interactive Brokers
    if not ib_connector.connect():
        print("Failed to connect to IB: ")
        return
        
    # Set IB connection in data fetcher
    data_fetcher.set_ib_connection(ib_connector.ib)
        
    risk_manager = RiskManager(
        max_position_size=0.02,
        stop_loss_pct=0.02,
        take_profit_pct=0.04
    )

    # 创建默认参数
    scanner_params = {
      'min_momentum_score': 0.6,
      'min_volume_score': 0.5,
      'max_volatility': 0.4,
      'max_positions': 5
    }
    
    # Start GUI with all required components
    gui = TradingGUI(
        data_fetcher=data_fetcher,
        ib_connector=ib_connector,
        risk_manager=risk_manager,
        strategies={
            'Momentum': momentum_strategy,
            'Mean Reversion': mean_reversion_strategy,
            'Statistical Arbitrage': statistical_arbitrage_strategy,
            'Market Scanner': market_scanner_strategy     # 添加新策略
        }
    )
    gui.run()

if __name__ == "__main__":
    main()