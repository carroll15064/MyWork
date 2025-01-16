class RiskManager:
    def __init__(self, max_position_size=0.02, stop_loss_pct=0.02, take_profit_pct=0.04):
        self.max_position_size = max_position_size  # 最大仓位比例
        self.stop_loss_pct = stop_loss_pct        # 止损百分比
        self.take_profit_pct = take_profit_pct    # 止盈百分比
        
    def calculate_position_size(self, account_value, current_price):
        """
        计算仓位大小（确保返回整数股数）
        """
        if current_price <= 0:
            return 0
            
        # 计算最大仓位金额
        max_position_value = account_value * self.max_position_size
        
        # 计算股数并向下取整
        shares = int(max_position_value / current_price)
        
        return max(1, shares)  # 确保至少返回1股
        
    def calculate_stop_loss(self, entry_price):
        """计算止损价格"""
        return entry_price * (1 - self.stop_loss_pct)
        
    def calculate_take_profit(self, entry_price):
        """计算止盈价格"""
        return entry_price * (1 + self.take_profit_pct)