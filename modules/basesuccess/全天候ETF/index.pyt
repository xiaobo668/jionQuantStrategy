from jqdata import *
from jqfactor import get_factor_values
import datetime
import numpy as np
import math
from scipy.optimize import minimize


# 初始化函数，设定基准等等
def initialize(context):
    # 设定沪深300作为基准
    # set_benchmark("515080.XSHG")
    # 打开防未来函数
    set_option("avoid_future_data", True)
    # 开启动态复权模式(真实价格)
    set_option("use_real_price", True)
    # 输出内容到日志 log.info()
    log.info("初始函数开始运行且全局只运行一次")
    # 过滤掉order系列API产生的比error级别低的log
    log.set_level("order", "error")
    # 固定滑点设置ETF 0.001(即交易对手方一档价)
    set_slippage(FixedSlippage(0.002), type="fund")
    # 股票交易总成本0.3%(含固定滑点0.02)
    set_slippage(FixedSlippage(0.02), type="stock")
    set_order_cost(
        OrderCost(
            open_tax=0,
            close_tax=0.001,
            open_commission=0.0003,
            close_commission=0.0003,
            close_today_commission=0,
            min_commission=5,
        ),
        type="stock",
    )
    # 设置货币ETF交易佣金0
    set_order_cost(
        OrderCost(
            open_tax=0,
            close_tax=0,
            open_commission=0,
            close_commission=0,
            close_today_commission=0,
            min_commission=0,
        ),
        type="mmf",
    )
    # 全局变量
    g.fill_stock = "511880.XSHG"  # 货币ETF,用于现金管理
    g.strategys = {}
    g.portfolio_value_proportion = [1]
    g.positions = {i: {} for i in range(len(g.portfolio_value_proportion))}  # 记录每个子策略的持仓股票

    # 策略变量
    g.jsg_signal = True  # 搅屎棍开仓信号

    # 初始化子策略对象（否则后续调仓会 KeyError）
    process_initialize(context)

    # 子策略执行计划
    if g.portfolio_value_proportion[0] > 0:
        run_daily(etf_rotation_adjust, "11:00")
    # 每日剩余资金购买货币ETF
    run_daily(end_trade, "14:59")


def process_initialize(context):
    print("重启程序")
    g.strategys["核心资产轮动策略"] = Etf_Rotation_Strategy(context, index=0, name="核心资产轮动策略")


# 尾盘处理
def end_trade(context):
    current_data = get_current_data()

    # 卖出未记录的股票（比如送股）
    keys = [key for d in g.positions.values() if isinstance(d, dict) for key in d.keys()]
    for stock in context.portfolio.positions:
        if stock not in keys and stock != g.fill_stock and current_data[stock].last_price < current_data[stock].high_limit:
            if order_target_value(stock, 0):
                log.info(f"卖出{stock}因送股未记录在持仓中")

    # 买入货币ETF
    amount = int(context.portfolio.available_cash / current_data[g.fill_stock].last_price)
    if amount >= 100:
        order(g.fill_stock, amount)


# 卖出货币ETF换现金
def get_cash(context, value):
    if g.fill_stock not in context.portfolio.positions:
        return
    current_data = get_current_data()
    amount = math.ceil(value / current_data[g.fill_stock].last_price / 100) * 100
    position = context.portfolio.positions[g.fill_stock].closeable_amount
    if amount >= 100:
        order(g.fill_stock, -min(amount, position))


def etf_rotation_adjust(context):
    g.strategys["核心资产轮动策略"].adjust()


# 策略基类
class Strategy:

    def __init__(self, context, index, name):
        self.context = context
        self.index = index
        self.name = name
        self.stock_sum = 1
        self.hold_list = []
        self.min_volume = 2000
        self.pass_months = [1, 4]
        self.def_stocks = ["511260.XSHG", "518880.XSHG", "512800.XSHG"]  # 债券ETF、黄金ETF、银行ETF

    # 获取策略当前持仓市值
    def get_total_value(self):
        if not g.positions[self.index]:
            return 0
        return sum(self.context.portfolio.positions[key].price * value for key, value in g.positions[self.index].items())

    # 卖出非连板股票，并且返回成功卖出的股票列表
    def _check(self):
        # 获取已持有列表
        self.hold_list = list(g.positions[self.index].keys())
        stocks = []
        # 获取昨日涨停、前日涨停昨日跌停列表
        if self.hold_list != []:
            df = get_price(
                self.hold_list,
                end_date=self.context.previous_date,
                frequency="daily",
                fields=["close", "high_limit"],
                count=3,
                panel=False,
                fill_paused=False,
            )
            df = df[df["close"] == df["high_limit"]]
            for stock in df.code.drop_duplicates():
                if self.order_target_value_(stock, 0):
                    stocks.append(stock)
        return stocks

    # 调仓(等权购买target中按顺序排列固定数量的的标的)
    def _adjust(self, target):

        # 获取已持有列表
        self.hold_list = list(g.positions[self.index].keys())

        # 调仓卖出
        for stock in self.hold_list:
            if stock not in target:
                self.order_target_value_(stock, 0)

        # 调仓买入
        target = [stock for stock in target if stock not in self.hold_list]
        sum = self.stock_sum - len(self.hold_list)
        self.buy(target[: min(len(target), sum)])

    # 调仓2(targets为字典，key为股票代码，value为目标市值)
    def _adjust2(self, targets):

        # 获取已持有列表
        self.hold_list = list(g.positions[self.index].keys())
        current_data = get_current_data()
        portfolio = self.context.portfolio

        # 清仓被调出的
        for stock in self.hold_list:
            if stock not in targets:
                self.order_target_value_(stock, 0)

        # 先卖出
        for stock, target in targets.items():
            price = current_data[stock].last_price
            value = g.positions[self.index].get(stock, 0) * price
            if value - target > self.min_volume and value - target > price * 100:
                self.order_target_value_(stock, target)

        # 后买入
        for stock, target in targets.items():
            price = current_data[stock].last_price
            value = g.positions[self.index].get(stock, 0) * price
            if target - value > self.min_volume and target - value > price * 100:
                if target - value > portfolio.available_cash:
                    get_cash(self.context, target - value - portfolio.available_cash)
                if portfolio.available_cash > price * 100 and portfolio.available_cash > self.min_volume:
                    self.order_target_value_(stock, target)

    # 可用现金等比例买入
    def buy(self, target):

        count = len(target)
        portfolio = self.context.portfolio

        # target为空或者持仓数量已满，不进行操作
        if count == 0 or self.stock_sum <= len(self.hold_list):
            return

        # 目标市值
        target_value = portfolio.total_value * g.portfolio_value_proportion[self.index]

        # 当前市值
        position_value = self.get_total_value()

        # 可用现金:当前现金 + 货币ETF市值
        available_cash = portfolio.available_cash + (portfolio.positions[g.fill_stock].value if g.fill_stock in portfolio.positions else 0)

        # 买入股票的总市值
        value = max(0, min(target_value - position_value, available_cash))

        # 卖出部分货币ETF获取现金
        if value > portfolio.available_cash:
            get_cash(self.context, value - portfolio.available_cash)

        # 等价值买入每一个未买入的标的
        for security in target:
            self.order_target_value_(security, value / count)

    # 自定义下单(涨跌停不交易)
    def order_target_value_(self, security, value):
        current_data = get_current_data()

        # 检查标的是否停牌、涨停、跌停
        if current_data[security].paused:
            log.info(f"{security}: 今日停牌")
            return False

        # 检查是否涨停
        if current_data[security].last_price == current_data[security].high_limit:
            log.info(f"{security}: 当前涨停")
            return False

        # 检查是否跌停
        if current_data[security].last_price == current_data[security].low_limit:
            log.info(f"{security}: 当前跌停")
            return False

        # 获取当前标的的价格
        price = current_data[security].last_price

        # 获取当前策略的持仓数量
        current_position = g.positions[self.index].get(security, 0)

        # 计算目标持仓数量
        target_position = (int(value / price) // 100) * 100 if price != 0 else 0

        # 计算需要调整的数量
        adjustment = target_position - current_position

        # 检查是否当天买入卖出
        closeable_amount = self.context.portfolio.positions[security].closeable_amount if security in self.context.portfolio.positions else 0
        if adjustment < 0 and closeable_amount == 0:
            log.info(f"{security}: 当天买入不可卖出")
            return False

        # 下单并更新持仓
        if adjustment != 0:
            o = order(security, adjustment)
            if o:
                # 更新持仓数量
                amount = o.amount if o.is_buy else -o.amount
                g.positions[self.index][security] = amount + current_position
                # 如果目标持仓为零，移除该证券
                if target_position == 0:
                    g.positions[self.index].pop(security, None)
                # 更新持有列表
                self.hold_list = list(g.positions[self.index].keys())
                return True
        return False

    # 基础过滤(过滤科创北交、ST、停牌、次新股)
    def filter_basic_stock(self, stock_list):

        current_data = get_current_data()
        return [
            stock
            for stock in stock_list
            if not current_data[stock].paused
            and not current_data[stock].is_st
            and "ST" not in current_data[stock].name
            and "*" not in current_data[stock].name
            and "退" not in current_data[stock].name
            and not (stock[0] == "4" or stock[0] == "8" or stock[:2] == "68")
            and not self.context.previous_date - get_security_info(stock).start_date < datetime.timedelta(375)
        ]

    # 过滤当前时间涨跌停的股票
    def filter_limitup_limitdown_stock(self, stock_list):
        current_data = get_current_data()
        return [
            stock
            for stock in stock_list
            if current_data[stock].last_price < current_data[stock].high_limit and current_data[stock].last_price > current_data[stock].low_limit
        ]

    # 判断今天是在空仓月
    def is_empty_month(self):
        month = self.context.current_dt.month
        return month in self.pass_months


# 核心资产轮动策略
class Etf_Rotation_Strategy(Strategy):
    def __init__(self, context, index, name):
        super().__init__(context, index, name)

        self.stock_sum = 1
        self.etf_pool = [
            # 境外
            "513100.XSHG",  # 纳指ETF
            '513500.XSHG',  # 标普ETF
            '164824.XSHE',  # 印度基金
            '513050.XSHG',  # 中概互联
            "513520.XSHG",  # 日经ETF
            "513030.XSHG",  # 德国ETF
            '513080.XSHG',  # 法国ETF
            # 商品
            "518880.XSHG",  # 黄金ETF
            "159980.XSHE",  # 有色ETF
            "159985.XSHE",  # 豆粕ETF
            "501018.XSHG",  # 南方原油
            # 债券
            "511010.XSHG",  # 国债ETF
            "511090.XSHG",  # 30年国债ETF
            # 国内
            "513130.XSHG",  # 恒生科技
            '510050.XSHG',  # 上证50ETF
            '512100.XSHG',  # 中证1000ETF
        ]
        self.m_days = 25  # 动量参考天数

    def get_etf_rank(self):
        scores = {}
        current_data = get_current_data()
        for etf in self.etf_pool:
            df = attribute_history(etf, self.m_days, "1d", ["close"])
            # prices = df["close"].values
            prices = np.append(df["close"].values, current_data[etf].last_price)
            y = np.log(prices)
            x = np.arange(len(y))
            weights = np.linspace(1, 2, len(y))
            slope, intercept = np.polyfit(x, y, 1, w=weights)
            annualized_returns = math.exp(slope * 250) - 1
            # 计算R²
            ss_res = np.sum(weights * (y - (slope * x + intercept)) ** 2)
            ss_tot = np.sum(weights * (y - np.mean(y)) ** 2)
            r2 = 1 - ss_res / ss_tot if ss_tot else 0
            scores[etf] = annualized_returns * r2
        
        print(scores)
        
        # 只保留得分在 (0, 5] 的ETF，并按得分降序排列
        rank_list = sorted(
            [etf for etf, score in scores.items() if 0 < score <= 6],
            key=lambda x: scores[x],
            reverse=True,
        )
        return rank_list

    def adjust(self):
        target = self.get_etf_rank()
        self._adjust(target[: min(self.stock_sum, len(target))])
