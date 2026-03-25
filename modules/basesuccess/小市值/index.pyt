# -*- coding: utf-8 -*-
from jqdata import * #聚宽数据接口
from jqfactor import * #聚宽因子接口
import numpy as np #numpy数据处理
import pandas as pd #pandas数据处理
from datetime import time, timedelta  # 修复：同时导入timedelta #时间处理

# 初始化函数 
def initialize(context):
    # 开启防未来函数
    # 作用：防止策略使用未来数据，目的是为了回测更真实，保证逻辑里只用已知数据，和实盘保持一致，避免过拟合
    set_option('avoid_future_data', True)
    # 设定基准
    # 作用：用于业绩对比（回测结果里会画出你的策略收益曲线和基准收益曲线，用来对比策略是否跑赢大盘）和计算超额收益
    # 例如：set_benchmark('399101.XSHE')，表示使用沪深300指数作为基准
    # 例如：set_benchmark('399006.XSHG')，表示使用创业板指数作为基准
    set_benchmark('399101.XSHE')
    # 用真实价格交易
    # 作用：使用真实价格交易，而不是模拟价格交易
    set_option('use_real_price', True)
    # 作用：滑点是指在交易时，由于市场价格波动，实际成交价格与预期价格之间的差异
    # 例如：set_slippage(PriceRelatedSlippage(0.002), type="stock")，表示滑点为0.002
    set_slippage(PriceRelatedSlippage(0.002), type="stock")
    # 设置交易成本
    # 作用：交易成本是指在交易时，由于市场价格波动，实际成交价格与预期价格之间的差异
    # open_tax：开仓印花税
    # close_tax：平仓印花税
    # open_commission：开仓佣金
    # close_commission：平仓佣金
    # close_today_commission：平今佣金
    # min_commission：最小佣金
    set_order_cost(
        OrderCost(
            open_tax=0,
            close_tax=0.0005,
            open_commission=0.0001,
            close_commission=0.0001,
            close_today_commission=0,
            min_commission=5,
        ),
        type="stock",
    )
    # 过滤order中低于error级别的日志
    log.set_level('order', 'error')
    log.set_level('system', 'error')
    log.set_level('strategy', 'debug')
    #初始化全局变量 bool
    g.no_trading_today_signal = False  # 是否为可交易日
    g.pass_april = True  # 是否四月空仓
    g.run_stoploss = True  # 是否进行止损
    #全局变量list
    g.hold_list = [] #当前持仓的全部股票    
    g.yesterday_HL_list = [] #记录持仓中昨日涨停的股票
    g.target_list = [] #目标股票列表
    g.not_buy_again = [] #不再买入的股票列表
    #全局变量
    g.stock_num = 6 #持仓股票数量
    g.up_price = 20  # 设置股票单价 
    g.limit_days_window = 3 * 250 # 历史涨停的参考窗口期
    g.init_stock_count = 1000 # 初始股池的数量
    g.reason_to_sell = ''
    g.stoploss_strategy = 3  # 1为止损线止损，2为市场趋势止损, 3为联合1、2策略
    g.stoploss_limit = 0.91  # 止损线 
    g.stoploss_market = 0.93  # 市场趋势止损参数
    
    g.HV_control = False #新增，Ture是日频判断是否放量，False则不然
    g.HV_duration = 120 #HV_control用，周期可以是240-120-60，默认比例是0.9
    g.HV_ratio = 0.9    #HV_control用
    g.stockL = [] #股票列表
    # g.no_trading_buy = ['600036.XSHG','518880.XSHG','600900.XSHG']  # 空仓月份持有 
    g.no_trading_buy = []  # 空仓月份持有  TODO
    g.no_trading_hold_signal = False
    # 设置交易运行时间
    run_daily(prepare_stock_list, '9:05') #准备股票池
    run_weekly(weekly_adjustment,2,'10:30') #每周调整持仓
    run_daily(sell_stocks, time='10:00') # 止损函数
    run_daily(trade_afternoon, time='14:20') #下午检查 上午交易
    run_daily(trade_afternoon, time='14:55') #下午检查 下午交易
    run_daily(close_account, '14:50') #收盘清仓
    # run_weekly(print_position_info, 5, time='15:10')


#1-1 准备股票池
def prepare_stock_list(context):
    #获取已持有列表
    g.hold_list= [] #持仓股票列表
    for position in list(context.portfolio.positions.values()): #获取持仓股票列表
        stock = position.security #股票代码
        g.hold_list.append(stock) #持仓股票列表
    #获取昨日涨停列表
    if g.hold_list != []: #如果持仓股票列表不为空
        df = get_price(g.hold_list, end_date=context.previous_date, frequency='daily', fields=['close','high_limit','low_limit'], count=1, panel=False, fill_paused=False) #获取昨日涨停列表
        df = df[df['close'] == df['high_limit']] #获取昨日涨停列表
        g.yesterday_HL_list = list(df.code) #昨日涨停列表
    else: #如果持仓股票列表为空
        g.yesterday_HL_list = [] #昨日涨停列表
    #判断今天是否为账户资金再平衡的日期
    g.no_trading_today_signal = today_is_between(context) #判断今天是否为账户资金再平衡的日期


def get_history_highlimit(context, stock_list, days=3*250, p=0.10):
    #获取历史涨停列表
    df = get_price( #获取历史涨停列表
        stock_list, #股票列表
        end_date=context.previous_date, #昨日日期
        frequency="daily", #每日数据
        fields=["close", "high_limit"], #收盘价和涨停价
        count=days, #历史窗口期
        panel=False, #不使用面板数据
    )
    df = df[df["close"] == df["high_limit"]] #获取历史涨停列表
    grouped_result = df.groupby('code').size().reset_index(name='count') #按股票代码分组，统计每个股票的涨停次数
    grouped_result = grouped_result.sort_values(by=["count"], ascending=False) #按涨停次数排序
    result_list = grouped_result["code"].tolist()[:int(len(grouped_result)*p)] #获取前p%的涨停股票
    log.info(f"筛选前合计{len(grouped_result)}个， 筛选后合计{len(result_list)}个") #打印筛选前和筛选后的股票数量
    return result_list #返回历史涨停列表
    
    
def get_start_point(context, stock_list, days=3*250):
    """修复版：显式按时间排序，确保逻辑正确性"""
    df = get_price(
        stock_list,
        end_date=context.previous_date,
        frequency="daily",
        fields=["open", "low", "close", "high_limit"],
        count=days,
        panel=False,
    )
    stock_start_point = {} #启动点
    stock_price_bias = {} #价格偏移量
    current_data = get_current_data() #获取当前数据
    
    for code, group in df.groupby('code'): #按股票代码分组
        # 第一步：关键修复！确保数据按时间顺序排列
        group = group.sort_values('time', ascending=True) #按时间排序
        
        # 找到所有close等于high_limit的行（涨停日）
        limit_hit_rows = group[group['close'] == group['high_limit']]
        
        if not limit_hit_rows.empty:
            # 第二步：按时间排序后，[-1]才是真正的"最近一次涨停"
            latest_limit_hit = limit_hit_rows.iloc[-1]
            latest_limit_date = latest_limit_hit.name  # 涨停发生的日期
            
            # 第三步：获取该涨停日之前的所有数据（包括涨停日）
            data_before_limit = group[group.index <= latest_limit_date]
            previous_rows = data_before_limit.iloc[::-1]  # 倒序排列，从涨停日开始往前查找
            
            # 寻找涨停后首次出现阴线（close < open）的K线
            for idx, row in previous_rows.iterrows():
                if row['close'] < row['open']:
                    # 将该阴线的最低价设为启动点
                    stock_start_point[code] = row['low']
                    break
    
    # 计算股票当前价格与历史启动点的偏移量
    for code, start_point in stock_start_point.items():
        last_price = current_data[code].last_price
        bias = last_price / start_point
        stock_price_bias[code] = bias
    
    # 按偏移量从小到大排序（最接近启动点的排前面）
    sorted_list = sorted(stock_price_bias.items(), key=lambda x: x[1], reverse=False)

    return [i[0] for i in sorted_list]

#1-2 选股模块
def get_stock_list(context): #获取股票列表
    final_list = [] #最终股票列表
    yesterday = context.previous_date #昨日日期
    initial_list = get_all_securities("stock", yesterday).index.tolist() #获取所有股票列表

    initial_list = filter_new_stock(context, initial_list) #过滤次新股
    initial_list = filter_kcbj_stock(initial_list) #过滤科创板、北交所股票
    initial_list = filter_st_stock(initial_list) #过滤ST股票
    initial_list = filter_paused_stock(initial_list) #过滤停牌股票
    
    q = query(
        valuation.code,indicator.eps #市值和每股收益
        ).filter(
            valuation.code.in_(initial_list) #在初始股票列表中
            ).order_by(
                valuation.market_cap.asc() #按市值从小到大排序
                )
    df = get_fundamentals(q) #获取基本面数据
    initial_list = df['code'].tolist()[:g.init_stock_count] #获取前g.init_stock_count只股票

    initial_list = filter_limitup_stock(context, initial_list) #过滤涨停股票
    initial_list = filter_limitdown_stock(context, initial_list) #过滤跌停股票
    
    initial_list = get_history_highlimit(context, initial_list, g.limit_days_window) #获取历史涨停股票
    initial_list = get_start_point(context, initial_list, g.limit_days_window) #获取启动点股票

    stock_list = get_stock_industry(initial_list) #获取行业股票列表
    final_list = stock_list[:g.stock_num*2]
    log.info('今日前10:%s' % final_list) #打印今日前10只股票
    
    return final_list #返回最终股票列表


#1-3 整体调整持仓
def weekly_adjustment(context):
    if g.no_trading_today_signal == False: #如果今天不是账户资金再平衡的日期
        close_no_trading_hold(context) #清仓非交易日的持仓
        #获取应买入列表 
        g.not_buy_again = [] #不再买入的股票列表
        g.target_list = get_stock_list(context) #获取目标股票列表
        target_list = g.target_list[:g.stock_num*2] #获取前g.stock_num*2只股票
        log.info(str(target_list)) #打印目标股票列表

        #调仓卖出
        for stock in g.hold_list: #遍历持仓股票列表
            if (stock not in target_list) and (stock not in g.yesterday_HL_list): #如果股票不在目标股票列表且不在昨日涨停股票列表
                log.info("卖出[%s]" % (stock)) #打印卖出股票
                position = context.portfolio.positions[stock] #获取股票持仓
                close_position(position) #卖出股票
            else:
                pass #如果股票在目标股票列表或昨日涨停股票列表，则不卖出
                log.info("已持有[%s]" % (stock)) #打印已持有股票
        #调仓买入
        buy_security(context,target_list) #买入股票 
        #记录已买入股票
        for position in list(context.portfolio.positions.values()): #遍历持仓股票列表
            stock = position.security #获取股票代码
            g.not_buy_again.append(stock) #记录已买入股票


#1-4 调整昨日涨停股票
def check_limit_up(context):
    now_time = context.current_dt #当前时间
    if g.yesterday_HL_list != []: #如果昨日涨停股票列表不为空
        #对昨日涨停股票观察到尾盘如不涨停则提前卖出，如果涨停即使不在应买入列表仍暂时持有
        for stock in g.yesterday_HL_list: #遍历昨日涨停股票列表
            if context.portfolio.positions[stock].closeable_amount > -100: #如果股票可卖出数量大于-100
                current_data = get_price(stock, end_date=now_time, frequency='1m', fields=['close','high_limit'], skip_paused=False, fq='pre', count=1, panel=False, fill_paused=True) #获取股票价格
                if current_data.iloc[0,0] <    current_data.iloc[0,1]:
                    log.info("[%s]涨停打开，卖出" % (stock))
                    position = context.portfolio.positions[stock] #获取股票持仓
                    close_position(position)
                    g.reason_to_sell = 'limitup'
                    # g.limitup_cash += context.portfolio.positions[stock].total_amount
                    # g.limitup_number += 1
                else:
                    log.info("[%s]涨停，继续持有" % (stock))


#1-5 如果昨天有股票卖出或者买入失败，剩余的金额今天早上买入
def check_remain_amount(context):
    if g.reason_to_sell == 'limitup': #判断提前售出原因，如果是涨停售出则次日再次交易，如果是止损售出则不交易
        g.hold_list= [] #持仓股票列表
        for position in list(context.portfolio.positions.values()): #遍历持仓股票列表
            stock = position.security #获取股票代码
            g.hold_list.append(stock) #持仓股票列表
        if len(g.hold_list) < g.stock_num: #如果持仓股票数量小于持仓股票数量
            target_list = get_stock_list(context) #获取目标股票列表
            target_list = filter_not_buy_again(target_list) #剔除本周一曾买入的股票，不再买入
            target_list = target_list[:min(g.stock_num, len(target_list))] #获取前g.stock_num只股票
            log.info('有余额可用'+str(round((context.portfolio.cash),2))+'元。'+ str(target_list)) #打印有余额可用
            buy_security(context,target_list) #买入股票
        g.reason_to_sell = '' #清空提前售出原因

    else:
        # log.info('虽然有余额（'+str(round((context.portfolio.cash),2))+'元）可用，但是为止损后余额，下周再交易')
        g.reason_to_sell = '' #清空提前售出原因


#1-6 下午检查交易
def trade_afternoon(context):
    if g.no_trading_today_signal == False: #如果今天不是账户资金再平衡的日期
        check_limit_up(context) #检查昨日涨停股票   
        if g.HV_control == True: #如果放量控制为True
            check_high_volume(context) #检查放量股票
        huanshou(context) #换手率控制
        
        check_remain_amount(context) #检查剩余金额
        
        
#1-7 止盈止损
def sell_stocks(context):
    if g.run_stoploss == True: #如果运行止损
        if g.stoploss_strategy == 1:
            for stock in context.portfolio.positions.keys(): #遍历持仓股票列表
                # 股票盈利大于等于100%则卖出
                if context.portfolio.positions[stock].price >= context.portfolio.positions[stock].avg_cost * 2: #如果股票盈利大于等于100%
                    order_target_value(stock, 0) #卖出股票 
                    log.debug("收益100%止盈,卖出{}".format(stock))
                # 止损
                elif context.portfolio.positions[stock].price < context.portfolio.positions[stock].avg_cost * g.stoploss_limit: #如果股票盈利小于止损线
                    order_target_value(stock, 0) #卖出股票 
                    log.debug("收益止损,卖出{}".format(stock))
                    g.reason_to_sell = 'stoploss'
        elif g.stoploss_strategy == 2:
            stock_df = get_price(security=get_index_stocks('399101.XSHE'), end_date=context.previous_date, frequency='daily', fields=['close', 'open'], count=1,panel=False) #获取指数数据
            #down_ratio = (stock_df['close'] / stock_df['open'] < 1).sum() / len(stock_df)
            #down_ratio = abs((stock_df['close'] / stock_df['open'] - 1).mean())
            down_ratio = (stock_df['close'] / stock_df['open']).mean() #计算指数平均涨跌幅
            if down_ratio <= g.stoploss_market: #如果指数平均涨跌幅小于等于止损线
                g.reason_to_sell = 'stoploss'
                log.debug("大盘惨跌,平均降幅{:.2%}".format(down_ratio))
                for stock in context.portfolio.positions.keys(): #遍历持仓股票列表
                    order_target_value(stock, 0)
        elif g.stoploss_strategy == 3:
            stock_df = get_price(security=get_index_stocks('399101.XSHE'), end_date=context.previous_date, frequency='daily', fields=['close', 'open'], count=1,panel=False) #获取指数数据
            #down_ratio = abs((stock_df['close'] / stock_df['open'] - 1).mean())
            down_ratio = (stock_df['close'] / stock_df['open']).mean() #计算指数平均涨跌幅
            if down_ratio <= g.stoploss_market:
                g.reason_to_sell = 'stoploss'
                log.debug("大盘惨跌,平均降幅{:.2%}".format(down_ratio))
                for stock in context.portfolio.positions.keys(): #遍历持仓股票列表
                    order_target_value(stock, 0) #卖出股票 
            else:
                for stock in context.portfolio.positions.keys(): #遍历持仓股票列表
                    if context.portfolio.positions[stock].price < context.portfolio.positions[stock].avg_cost * g.stoploss_limit: #如果股票盈利小于止损线
                        order_target_value(stock, 0) #卖出股票  
                        log.debug("收益止损,卖出{}".format(stock))
                        g.reason_to_sell = 'stoploss'
                        

# 3-2 调整放量股票
def check_high_volume(context):
    current_data = get_current_data() #获取当前数据
    for stock in context.portfolio.positions:
        if current_data[stock].paused == True: #如果股票停牌
            continue
        if current_data[stock].last_price == current_data[stock].high_limit: #如果股票涨停
            continue
        if context.portfolio.positions[stock].closeable_amount ==0: #如果股票可卖出数量为0
            continue
        df_volume = get_bars(stock,count=g.HV_duration,unit='1d',fields=['volume'],include_now=True, df=True) #获取股票成交量
        if df_volume['volume'].values[-1] > g.HV_ratio*df_volume['volume'].values.max(): #如果股票成交量大于最大成交量的g.HV_ratio倍
            position = context.portfolio.positions[stock] #获取股票持仓
            r = close_position(position) #卖出股票
            log.info(f"[{stock}]天量，卖出, close_position: {r}") #打印卖出股票
            g.reason_to_sell is 'limitup' # TODO 涨停卖出

            
            
#2-1 过滤停牌股票
def filter_paused_stock(stock_list):
    current_data = get_current_data() #获取当前数据 
    return [stock for stock in stock_list if not current_data[stock].paused] #过滤停牌股票



#2-2 过滤ST及其他具有退市标签的股票
def filter_st_stock(stock_list):
    current_data = get_current_data() #获取当前数据
    return [stock for stock in stock_list #过滤ST股票
            if not current_data[stock].is_st
            and 'ST' not in current_data[stock].name #过滤ST股票
            and '*' not in current_data[stock].name #过滤*股票
            and '退' not in current_data[stock].name] #过滤退市股票


#2-3 过滤科创北交股票
def filter_kcbj_stock(stock_list):
    for stock in stock_list[:]: #遍历股票列表
        if stock[0] == '4' or stock[0] == '8' or stock[:2] == '68': #过滤科创板、北交所股票
            stock_list.remove(stock) #删除股票
    return stock_list #返回股票列表


#2-4 过滤涨停的股票
def filter_limitup_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list) #获取股票价格
    current_data = get_current_data() #获取当前数据
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys() #过滤持仓股票
            or last_prices[stock][-1] <    current_data[stock].high_limit] #过滤涨停股票


#2-5 过滤跌停的股票
def filter_limitdown_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list) #获取股票价格
    current_data = get_current_data() #获取当前数据
    return [stock for stock in stock_list if (stock in context.portfolio.positions.keys() #过滤持仓股票
            or last_prices[stock][-1] > current_data[stock].low_limit) #过滤跌停股票
            ]


#2-6 过滤次新股
def filter_new_stock(context,stock_list):
    yesterday = context.previous_date #昨日日期
    # 修复：使用正确的timedelta调用方式
    return [stock for stock in stock_list if not yesterday - get_security_info(stock).start_date < timedelta(days=375)] #过滤次新股


#2-6.5 过滤股价
def filter_highprice_stock(context,stock_list):
	last_prices = history(1, unit='1m', field='close', security_list=stock_list) #获取股票价格
	return [stock for stock in stock_list if stock in context.portfolio.positions.keys() #过滤持仓股票
			or last_prices[stock][-1] <= g.up_price]


#2-7 删除本周一买入的股票
def filter_not_buy_again(stock_list):
    return [stock for stock in stock_list if stock not in g.not_buy_again] #过滤已买入股票
    
    
# 获取股票所属行业
def get_stock_industry(stock):
    result = get_industry(security=stock) #获取股票所属行业
    selected_stocks = [] #选中的股票列表
    industry_list = [] #行业列表

    for stock_code, info in result.items():
        industry_name = info['sw_l2']['industry_name'] #获取行业名称
        if industry_name not in industry_list: #如果行业名称不在行业列表中
            industry_list.append(industry_name) #添加行业名称
            selected_stocks.append(stock_code) #添加股票代码
            # print(f"行业信息: {industry_name} (股票: {stock_code})")
            # 选取了 10 个不同行业的股票
            if len(industry_list) == 10 :
                break
    return selected_stocks

            
#换手率计算
def huanshoulv(context, stock, is_avg=False):
    if is_avg: #如果计算平均换手率
        # 计算平均换手率
        start_date = context.current_dt - datetime.timedelta(days=20)
        end_date = context.previous_date #昨日日期
        df_volume = get_price(stock,end_date=end_date, frequency='daily', fields=['volume'],count=20) #获取股票成交量
        df_cap = get_valuation(stock, end_date=end_date, fields=['circulating_cap'], count=1) #获取股票流通市值
        circulating_cap = df_cap['circulating_cap'].iloc[0] if not df_cap.empty else 0 #如果流通市值为空，则返回0   
        if circulating_cap == 0: #如果流通市值为0，则返回0
            return 0.0 #如果流通市值为0，则返回0
        df_volume['turnover_ratio'] = df_volume['volume'] / (circulating_cap * 10000) #计算换手率
        return df_volume['turnover_ratio'].mean() #返回换手率
    else:
        # 计算实时换手率
        date_now = context.current_dt #当前日期
        df_vol = get_price(stock, start_date=date_now.date(), end_date=date_now, frequency='1m', fields=['volume'],
                           skip_paused=False, fq='pre', panel=True, fill_paused=False) #获取股票成交量
        volume = df_vol['volume'].sum() #获取股票成交量
        date_pre = context.previous_date #昨日日期
        df_circulating_cap = get_valuation(stock, end_date=date_pre, fields=['circulating_cap'], count=1) #获取股票流通市值
        circulating_cap = df_circulating_cap['circulating_cap'].iloc[0]  if not df_circulating_cap.empty else 0 #如果流通市值为空，则返回0
        if circulating_cap == 0: #如果流通市值为0，则返回0
            return 0.0 #如果流通市值为0，则返回0
        turnover_ratio = volume / (circulating_cap * 10000) #计算换手率
        return turnover_ratio #返回换手率            


# 换手检测
def huanshou(context):
    ss = [] #股票列表
    current_data = get_current_data() #获取当前数据
    shrink, expand = 0.003, 0.1 #缩量和放量阈值
    for stock in context.portfolio.positions: #遍历持仓股票列表
        if current_data[stock].paused == True: #如果股票停牌
            continue
        if current_data[stock].last_price >= current_data[stock].high_limit*0.97: #如果股票涨停
            continue
        if context.portfolio.positions[stock].closeable_amount ==0: #如果股票可卖出数量为0
            continue
        rt = huanshoulv(context, stock, False)
        avg = huanshoulv(context, stock, True) #计算平均换手率
        if avg == 0: continue #如果平均换手率为0，则继续
        r = rt / avg #计算换手率倍率
        action, icon = '', '' #动作和图标
        if avg < 0.003: #如果平均换手率小于0.003，则缩量
            action, icon = '缩量', '❄️'
        elif rt > expand and r > 2: #如果换手率倍率大于2，则放量    
            action, icon = '放量', '🔥' #如果换手率倍率大于2，则放量    
        if action:
            position = context.portfolio.positions[stock] #获取股票持仓
            r = close_position(position) #卖出股票  
            log.info(f"{action} {stock} {get_security_info(stock).display_name} 换手率:{rt:.2%}→均:{avg:.2%} 倍率:{r:.1f}x {icon} close_position: {r}") #打印卖出股票   
            g.reason_to_sell = 'limitup' #涨停卖出
            
            
#3-1 交易模块-自定义下单
def order_target_value_(security, value):
    if value == 0: #如果价值为0，则不卖出
        pass
        #log.debug("Selling out %s" % (security)) #打印卖出股票
    else:
        pass
        # log.debug("Order %s to value %f" % (security, value)) #打印卖出股票
    return order_target_value(security, value) #卖出股票

#3-2 交易模块-开仓
def open_position(security, value):
    order = order_target_value_(security, value) #卖出股票  
    if order != None and order.filled > 0: #如果订单不为空且已成交
        return True #返回True
    return False #返回False

#3-3 交易模块-平仓
def close_position(position):
    security = position.security #获取股票代码
    order = order_target_value_(security, 0)  # 可能会因停牌失败
    if order != None: #如果订单不为空
        if order.status == OrderStatus.held and order.filled == order.amount: #如果订单状态为已成交且数量等于订单数量
            return True #返回True
    return False #返回False

#3-4 买入模块
def buy_security(context,target_list,cash=0,buy_number=0):
    #调仓买入
    position_count = len(context.portfolio.positions) #获取持仓股票数量
    target_num = g.stock_num #获取目标股票数量
    if cash == 0: #如果现金为0，则现金为总市值
        cash = context.portfolio.total_value #cash
    if buy_number == 0:
        buy_number = target_num #如果买入数量为0，则买入数量为目标股票数量
    bought_num = 0 #已买入股票数量
    print('---------------------buy_number：%s'%buy_number) #打印买入数量   
    if target_num > position_count: #如果目标股票数量大于持仓股票数量
        value = cash / (target_num) # - position_count #计算买入金额
        for stock in target_list: #遍历目标股票列表
            if context.portfolio.positions[stock].total_amount == 0: #如果股票持仓数量为0
            #if stock not in context.portfolio.positions:
                if bought_num < buy_number: #如果已买入股票数量小于买入数量
                    if open_position(stock, value):
                        # log.info("买入[%s]（%s元）" % (stock,value))
                        g.not_buy_again.append(stock) #持仓清单，后续不希望再买入
                        bought_num += 1 #已买入股票数量加1  
                        if len(context.portfolio.positions) == target_num: #如果持仓股票数量等于目标股票数量
                            break
    # else:
    #     value = cash / target_num
    #     for stock in target_list:
    #         if context.portfolio.positions[stock].total_amount == 0:
    #             if bought_num < buy_number:
    #                 if open_position(stock, value):
    #                     log.info("买入[%s]（%s元）" % (stock,value))
    #                     g.not_buy_again.append(stock) #持仓清单，后续不希望再买入
    #                     bought_num += 1
    #                     if len(context.portfolio.positions) == target_num:
    #                         break




#4-1 判断今天是否为四月
def today_is_between(context):
    today = context.current_dt.strftime('%m-%d')
    if g.pass_april is True:
        if (('04-01' <= today) and (today <= '04-30')):
            return True
        else:
           return False
    else:
        return False


#4-2 清仓后次日资金可转
def close_account(context): 
    if g.no_trading_today_signal == True: #如果今天不是账户资金再平衡的日期
        if len(g.hold_list) != 0 and g.no_trading_hold_signal == False:
            for stock in g.hold_list: #遍历持仓股票列表
                position = context.portfolio.positions[stock] #获取股票持仓
                if close_position(position): #卖出股票
                    log.info("卖出[%s]" % (stock)) #打印卖出股票
                else:
                    log.info("卖出[%s]错误！！！！！" % (stock)) #打印卖出股票错误
            buy_security(context, g.no_trading_buy) #买入股票
            g.no_trading_hold_signal = True    #账户资金再平衡信号
            

#4-3 清仓小市值不交易期间股票
def close_no_trading_hold(context): #清仓小市值不交易期间股票
    if g.no_trading_hold_signal == True: #如果账户资金再平衡信号为True
        for stock in g.hold_list: #遍历持仓股票列表
            position = context.portfolio.positions[stock] #获取股票持仓
            close_position(position) #卖出股票
            log.info("卖出[%s]" % (stock)) #打印卖出股票
        g.no_trading_hold_signal = False #账户资金再平衡信号



def print_position_info(context): #打印持仓股票信息
    print('———————————————————————————————————')
    for position in list(context.portfolio.positions.values()): #遍历持仓股票列表
        securities=position.security #获取股票代码
        cost=position.avg_cost #获取股票成本
        price=position.price #获取股票价格
        ret=100*(price/cost-1) #获取股票收益率
        value=position.value #获取股票市值
        amount=position.total_amount #获取股票持仓数量
        print('代码:{}'.format(securities)) #打印股票代码
        print('收益率:{}%'.format(format(ret,'.2f'))) #打印股票收益率
        print('持仓(股):{}'.format(amount)) #打印股票持仓数量
        print('市值:{}'.format(format(value,'.2f'))) #打印股票市值
        print('———————————————————————————————————') #打印分割线
    print('余额:{}'.format(format(context.portfolio.cash,'.2f'))) #打印余额
    print('———————————————————————————————————————分割线————————————————————————————————————————') #打印分割线