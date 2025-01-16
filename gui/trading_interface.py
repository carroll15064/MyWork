import tkinter as tk
from tkinter import ttk, messagebox
from trading.trade_manager import TradeManager

class TradingGUI:
    def __init__(self, data_fetcher, ib_connector, risk_manager, strategies):
        # Initialize components
        self.data_fetcher = data_fetcher
        self.ib_connector = ib_connector
        self.risk_manager = risk_manager
        self.strategies = strategies
        
        # Create main window
        self.root = tk.Tk()
        self.root.title("量化交易系统")
        self.root.geometry("1024x768")
        
        # Configure style
        self.style = ttk.Style()
        self.style.theme_use('clam')  # 使用更现代的clam主题
        
        # 配置自定义样式
        self.style.configure("Header.TLabel", 
                           font=('Helvetica', 14, 'bold'),
                           padding=10)
        self.style.configure("Status.TLabel", 
                           font=('Helvetica', 10),
                           padding=5)
        self.style.configure("Info.TLabel",
                           font=('Helvetica', 10),
                           foreground='#666666')
        self.style.configure("Success.TLabel",
                           foreground='green')
        self.style.configure("Error.TLabel",
                           foreground='red')
        self.style.configure("Primary.TButton",
                           font=('Helvetica', 10),
                           padding=5)
        self.style.configure("Accent.TButton",
                           font=('Helvetica', 10, 'bold'),
                           padding=5)
        
        # Initialize Tkinter variables
        self.strategy_var = tk.StringVar()
        self.ticker_var = tk.StringVar()
        self.ticker2_var = tk.StringVar()
        self.auto_trading_var = tk.BooleanVar(value=False)
        
        # 创建交易管理器
        self.trade_manager = TradeManager(ib_connector, risk_manager, data_fetcher)
        
        # Create main container with padding
        self.main_container = ttk.Frame(self.root, padding="20")
        self.main_container.pack(fill=tk.BOTH, expand=True)
        
        # Create GUI elements
        self.create_header()
        self.create_strategy_section()
        self.create_trading_controls()
        self.create_status_bar()
        
    def create_header(self):
        """创建头部区域"""
        header_frame = ttk.Frame(self.main_container)
        header_frame.pack(fill=tk.X, pady=(0, 20))
        
        # Logo/Title
        title_label = ttk.Label(
            header_frame, 
            text="量化交易系统",
            style="Header.TLabel"
        )
        title_label.pack(side=tk.LEFT)
        
        # Connection status
        self.connection_label = ttk.Label(
            header_frame,
            text="● 已连接",
            style="Success.TLabel"
        )
        self.connection_label.pack(side=tk.RIGHT, padx=10)
        
    def create_strategy_section(self):
        """创建策略选择区域"""
        strategy_frame = ttk.LabelFrame(
            self.main_container,
            text="策略配置",
            padding="15"
        )
        strategy_frame.pack(fill=tk.X, pady=(0, 15))
        
        # Strategy selection
        strategy_select_frame = ttk.Frame(strategy_frame)
        strategy_select_frame.pack(fill=tk.X, pady=(0, 15))
        
        ttk.Label(
            strategy_select_frame,
            text="选择策略:",
            style="Info.TLabel"
        ).pack(side=tk.LEFT, padx=(0, 10))
        
        strategy_dropdown = ttk.Combobox(
            strategy_select_frame,
            textvariable=self.strategy_var,
            values=list(self.strategies.keys()),
            width=30,
            state="readonly"
        )
        strategy_dropdown.pack(side=tk.LEFT)
        strategy_dropdown.bind('<<ComboboxSelected>>', self.on_strategy_select)
        
        # Ticker inputs
        ticker_frame = ttk.Frame(strategy_frame)
        ticker_frame.pack(fill=tk.X, pady=(0, 5))
        
        # First ticker
        ttk.Label(
            ticker_frame,
            text="交易标的 1:",
            style="Info.TLabel"
        ).pack(side=tk.LEFT, padx=(0, 10))
        
        self.ticker_entry = ttk.Entry(
            ticker_frame,
            textvariable=self.ticker_var,
            width=15
        )
        self.ticker_entry.pack(side=tk.LEFT, padx=(0, 20))
        
        # Second ticker (initially hidden)
        self.ticker2_label = ttk.Label(
            ticker_frame,
            text="交易标的 2:",
            style="Info.TLabel"
        )
        self.ticker2_entry = ttk.Entry(
            ticker_frame,
            textvariable=self.ticker2_var,
            width=15
        )
        
    def create_trading_controls(self):
        """创建交易控制面板"""
        control_frame = ttk.LabelFrame(
            self.main_container,
            text="交易控制",
            padding="15"
        )
        control_frame.pack(fill=tk.X, pady=(0, 15))
        
        # Auto trading controls
        auto_trade_frame = ttk.Frame(control_frame)
        auto_trade_frame.pack(fill=tk.X, pady=(0, 15))
        
        ttk.Checkbutton(
            auto_trade_frame,
            text="启用自动交易",
            variable=self.auto_trading_var,
            command=self.toggle_auto_trading,
            style="Primary.TCheckbutton"
        ).pack(side=tk.LEFT, padx=(0, 20))
        
        self.status_label = ttk.Label(
            auto_trade_frame,
            text="自动交易状态: 已禁用",
            style="Info.TLabel"
        )
        self.status_label.pack(side=tk.LEFT)
        
        # Manual control buttons
        button_frame = ttk.Frame(control_frame)
        button_frame.pack(fill=tk.X, pady=(10, 0))
        
        ttk.Button(
            button_frame,
            text="执行单次策略",
            command=self.execute_strategy,
            style="Accent.TButton"
        ).pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Button(
            button_frame,
            text="清空持仓",
            command=self.clear_positions,
            style="Primary.TButton"
        ).pack(side=tk.LEFT)
        
    def create_status_bar(self):
        """创建状态栏"""
        status_frame = ttk.Frame(self.root, padding="5")
        status_frame.pack(fill=tk.X, side=tk.BOTTOM)
        
        self.status_message = ttk.Label(
            status_frame,
            text="就绪",
            style="Info.TLabel"
        )
        self.status_message.pack(side=tk.LEFT, padx=10)
        
    def clear_positions(self):
        """清空所有持仓"""
        if messagebox.askyesno("确认", "确定要清空所有持仓吗？"):
            try:
                self.trade_manager.clear_all_positions()
                self.update_status("所有持仓已清空")
                messagebox.showinfo("成功", "所有持仓已成功清空")
            except Exception as e:
                messagebox.showerror("错误", f"清空持仓时发生错误: {str(e)}")
                
    def update_status(self, message, status_type="info"):
        """更新状态栏消息"""
        style = {
            "info": "Info.TLabel",
            "success": "Success.TLabel",
            "error": "Error.TLabel"
        }.get(status_type, "Info.TLabel")
        
        self.status_message.configure(style=style, text=message)
        
    def on_strategy_select(self, event=None):
        """当策略选择改变时调用"""
        selected_strategy = self.strategy_var.get()
        
        # 如果是统计套利策略，显示第二个股票输入
        if selected_strategy == "Statistical Arbitrage":
            self.ticker2_label.pack(side=tk.LEFT, padx=(0, 10))
            self.ticker2_entry.pack(side=tk.LEFT)
        else:
            self.ticker2_label.pack_forget()
            self.ticker2_entry.pack_forget()
            
    def toggle_auto_trading(self):
        """切换自动交易状态"""
        try:
            if self.auto_trading_var.get():
                selected_strategy = self.strategy_var.get()
                ticker1 = self.ticker_var.get()
                
                if not selected_strategy or not ticker1:
                    raise ValueError("请选择策略并输入交易标的")
                    
                # 准备策略参数
                if selected_strategy == "Statistical Arbitrage":
                    ticker2 = self.ticker2_var.get()
                    if not ticker2:
                        raise ValueError("请输入第二个交易标的")
                    params = {'ticker1': ticker1, 'ticker2': ticker2}
                else:
                    params = {'ticker': ticker1}
                    
                # 启动自动交易
                success = self.trade_manager.start_auto_trading(
                    selected_strategy,
                    self.strategies[selected_strategy],
                    params
                )
                
                if success:
                    self.status_label.configure(
                        text="自动交易状态: 运行中",
                        style="Success.TLabel"
                    )
                    self.update_status("自动交易已启动", "success")
                    messagebox.showinfo("成功", "自动交易已启动")
                else:
                    raise ValueError("启动自动交易失败")
                    
            else:
                # 停止自动交易
                self.trade_manager.stop_auto_trading()
                self.status_label.configure(
                    text="自动交易状态: 已停止",
                    style="Info.TLabel"
                )
                self.update_status("自动交易已停止")
                messagebox.showinfo("成功", "自动交易已停止")
                
        except Exception as e:
            self.auto_trading_var.set(False)
            self.status_label.configure(
                text="自动交易状态: 错误",
                style="Error.TLabel"
            )
            self.update_status(f"错误: {str(e)}", "error")
            messagebox.showerror("错误", f"自动交易错误: {str(e)}")
            
    def execute_strategy(self):
        """执行单次策略"""
        try:
            selected_strategy = self.strategy_var.get()
            ticker1 = self.ticker_var.get()
            
            if not ticker1:
                raise ValueError("请输入交易标的")
                
            if not selected_strategy:
                raise ValueError("请选择策略")
                
            if selected_strategy in self.strategies:
                strategy_func = self.strategies[selected_strategy]
                
                # 准备参数
                if selected_strategy == "Statistical Arbitrage":
                    ticker2 = self.ticker2_var.get()
                    if not ticker2:
                        raise ValueError("请输入第二个交易标的")
                    params = {'ticker1': ticker1, 'ticker2': ticker2}
                else:
                    params = {'ticker': ticker1}
                
                # 执行策略
                result = strategy_func(
                    data_fetcher=self.data_fetcher,
                    ib_connector=self.ib_connector,
                    risk_manager=self.risk_manager,
                    params=params
                )
                
                # 显示执行结果
                if result:
                    self.update_status(f"策略 {selected_strategy} 执行成功", "success")
                    messagebox.showinfo(
                        "策略执行结果",
                        f"策略: {selected_strategy}\n" +
                        f"信号: {result.get('signal', 'N/A')}\n" +
                        (f"交易对: {result.get('pair', ('N/A', 'N/A'))}\n" 
                         if selected_strategy == "Statistical Arbitrage" else "")
                    )
            else:
                raise ValueError("无效的策略选择")
                
        except Exception as e:
            self.update_status(f"错误: {str(e)}", "error")
            messagebox.showerror("错误", f"策略执行错误: {str(e)}")
            
    def run(self):
        """启动 GUI"""
        # 设置窗口最小尺寸
        self.root.minsize(800, 600)
        # 启动主循环
        self.root.mainloop()