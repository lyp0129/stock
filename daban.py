#!/usr/bin/env python
# coding: utf-8

import tushare as ts
import pandas as pd
import numpy as np
import time
import os
import sys
from datetime import datetime, timedelta
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import threading
import random
from tqdm import tqdm
import requests  # 添加到文件顶部的导入部分


class StockMonitor:
    def __init__(self, stock_list, upper_limit=0.1, lower_limit=-0.1):
        ts.set_token('854634d420c0b6aea2907030279da881519909692cf56e6f35c4718c')
        self.pro = ts.pro_api()
        self.stock_list = stock_list
        self.upper_limit = upper_limit
        self.lower_limit = lower_limit
        self.last_prices = {}
        self.current_batch = 0  # 追踪当前批次
        self.lock = threading.Lock()  # 添加线程锁
        self.hot_concepts = {}  # 存储关联板块信息
        self.first_limit_up_stocks = set()  # 存储3天内首次涨停的股票
        self.related_stocks = {}  # 存储股票关联关系
        self.all_stocks_data = None  # 初始化为 None，而不是空 DataFrame

        # 修改飞书配置，使用 webhook
        self.feishu_webhook = "https://open.feishu.cn/open-apis/bot/v2/hook/4ae401fd-fb8f-490b-b496-f437e8b15227"

    def get_batch_realtime_data(self, batch_size=50):
        """获取下一批股票数据"""
        start_idx = self.current_batch * batch_size
        end_idx = start_idx + batch_size

        # 如果到达列表末尾，重新从开始循环
        if start_idx >= len(self.stock_list):
            self.current_batch = 0
            start_idx = 0
            end_idx = batch_size
            print("\n已监控完所有股票，重新开始第一批...")

        stocks_to_monitor = self.stock_list[start_idx:end_idx]
        self.current_batch += 1

        try:
            stock_str = ','.join(stocks_to_monitor)
            df = ts.realtime_quote(ts_code=stock_str)
            if df is not None and not df.empty:
                print(f"\n正在监控第 {self.current_batch} 批股票 (总共 {len(self.stock_list)} 只)")
                return df
            return pd.DataFrame()
        except Exception as e:
            print(f"获取批次数据失败: {str(e)}")
            return pd.DataFrame()

    def get_minute_vol(self, ts_code, limit=30):
        """获取最近30分钟的分钟级别数据"""
        try:
            # 修正pro_bar的调用方式
            df = ts.pro_bar(
                ts_code=ts_code,
                freq='1min',
                start_date=datetime.now().strftime('%Y%m%d'),
                end_date=datetime.now().strftime('%Y%m%d'),
                asset='E',
                adj='qfq'
            )

            if df is not None and not df.empty:
                # 计算最高成交额和对应时间
                max_amount = df['amount'].max()
                max_amount_time = df.loc[df['amount'] == max_amount, 'trade_time'].iloc[0]
                latest_amount = df['amount'].iloc[0]  # 最新一分钟成交额

                return {
                    'latest_amount': latest_amount / 10000,  # 转换为万元
                    'max_amount': max_amount / 10000,
                    'max_amount_time': max_amount_time
                }
        except Exception as e:
            print(f"获取{ts_code}分钟数据失败: {str(e)}")
        return None

    def is_trading_time(self):
        """判断当前是否为交易时间"""
        now = datetime.now().time()
        
        # 定义交易时间段
        morning_start = datetime.strptime('09:30:00', '%H:%M:%S').time()
        morning_end = datetime.strptime('11:30:00', '%H:%M:%S').time()
        afternoon_start = datetime.strptime('13:00:00', '%H:%M:%S').time()
        afternoon_end = datetime.strptime('21:00:00', '%H:%M:%S').time()
        
        # 判断是否在交易时间内
        is_morning_trading = morning_start <= now <= morning_end
        is_afternoon_trading = afternoon_start <= now <= afternoon_end
        
        return is_morning_trading or is_afternoon_trading

    def monitor(self, interval=1):
        """修改监控函数，添加交易时间判断和开盘提醒"""
        last_status = False  # 记录上一次的交易状态
        
        while True:
            try:
                current_status = self.is_trading_time()
                
                # 检测是否刚开盘（状态从非交易变为交易）
                if current_status and not last_status:
                    current_time = datetime.now().strftime('%H:%M:%S')
                    print(f"\n市场开盘了！当前时间: {current_time}")
                    
                    # 发送飞书通知
                    message = (f"🔔 股票市场开盘提醒\n"
                              f"当前时间: {current_time}\n"
                              f"监控股票数量: {len(self.stock_list)}")
                    
                    data = {
                        "msg_type": "text",
                        "content": {
                            "text": message
                        }
                    }
                    try:
                        response = requests.post(self.feishu_webhook, json=data)
                        if response.status_code != 200:
                            print(f"发送飞书消息失败: {response.text}")
                    except Exception as e:
                        print(f"发送飞书消息异常: {str(e)}")
                
                # 更新状态
                last_status = current_status
                
                # 非交易时间处理
                if not current_status:
                    current_time = datetime.now().strftime('%H:%M:%S')
                    print(f"\r当前时间 {current_time} 不在交易时间内，等待中...", end='')
                    time.sleep(60)  # 非交易时间每分钟检查一次
                    continue
                    
                # 使用线程池并行获取数据
                with ThreadPoolExecutor(max_workers=5) as executor:
                    futures = []
                    for i in range(0, len(self.stock_list), 50):
                        batch = self.stock_list[i:i + 50]
                        futures.append(executor.submit(self.get_batch_data, batch))

                    # 收集所有批次的数据
                    all_data = []
                    for future in concurrent.futures.as_completed(futures):
                        result = future.result()
                        if result is not None and not result.empty:
                            all_data.append(result)

                    if all_data:
                        # 合并所有数据
                        df = pd.concat(all_data, ignore_index=True)

                        # 处理数据
                        df['price'] = pd.to_numeric(df['price'], errors='coerce')
                        df['pre_close'] = pd.to_numeric(df['pre_close'], errors='coerce')
                        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
                        df['pct_chg'] = (df['price'] - df['pre_close']) / df['pre_close'] * 100

                        # 找出涨停股票
                        limit_up_stocks = df[df['pct_chg'] >= 9.5].copy()

                        if not limit_up_stocks.empty:
                            print(f"\n发现 {len(limit_up_stocks)} 只涨停股票")

                            # 获取所有涨停股票的概念信息
                            concept_groups = {}  # 存储每个概念下的涨停股票
                            concept_codes = {}  # 存储概念名称到代码的映射
                            other_stocks = limit_up_stocks.copy()  # 用于存储不属于热门概念的涨停股

                            for _, stock in limit_up_stocks.iterrows():
                                try:
                                    concepts = self.pro.concept_detail(ts_code=stock['ts_code'])
                                    if concepts is not None and not concepts.empty:
                                        for _, concept in concepts.iterrows():
                                            concept_name = concept['concept_name']
                                            concept_code = concept['id']
                                            if concept_name not in concept_groups:
                                                concept_groups[concept_name] = []
                                                concept_codes[concept_name] = concept_code
                                            concept_groups[concept_name].append(stock)
                                except Exception as e:
                                    print(f"获取概念信息失败: {str(e)}")

                            # 筛选涨停数量大于等于3的概念
                            hot_concepts = {k: v for k, v in concept_groups.items() if len(v) >= 3}

                            # 从其他股票中移除属于热门概念的股票
                            if hot_concepts:
                                hot_stocks = set()
                                for stocks in hot_concepts.values():
                                    hot_stocks.update([stock['ts_code'] for stock in stocks])
                                other_stocks = other_stocks[~other_stocks['ts_code'].isin(hot_stocks)]

                            # 先显示热门概念板块
                            if hot_concepts:
                                print("\n=== 热门概念板块（涨停数量>=3）===")
                                for concept_name, stocks in hot_concepts.items():
                                    print(f"\n【{concept_name}】概念 已有{len(stocks)}只涨停")
                                    print("=" * 80)

                                    print("\n涨停股票:")
                                    print("-" * 80)
                                    for stock in stocks:
                                        print(f"{stock['ts_code']} {stock['name']} "
                                              f"涨幅: {stock['pct_chg']:.2f}% "
                                              f"成交额: {float(stock['amount']) / 10000:.2f}万")

                                    # 获取同概念未涨停的潜力股
                                    try:
                                        concept_code = concept_codes.get(concept_name)
                                        if concept_code:
                                            concept_stocks = self.pro.concept_detail(id=concept_code)
                                            if concept_stocks is not None and not concept_stocks.empty:
                                                potential_stocks = df[
                                                    (df['ts_code'].isin(concept_stocks['ts_code'])) &
                                                    (df['pct_chg'] >= 6.0) &
                                                    (df['pct_chg'] < 9.5)
                                                    ]

                                                if not potential_stocks.empty:
                                                    print("\n同概念潜力股(涨幅6%-9.5%):")
                                                    print("-" * 80)
                                                    for _, pot_stock in potential_stocks.iterrows():
                                                        print(f"{pot_stock['ts_code']} {pot_stock['name']} "
                                                              f"涨幅: {pot_stock['pct_chg']:.2f}% "
                                                              f"成交额: {float(pot_stock['amount']) / 10000:.2f}万")

                                                    # 发送飞书通知
                                                    self.send_feishu_message(concept_name, stocks, potential_stocks)

                                    except Exception as e:
                                        print(f"获取同概念股票失败: {str(e)}")

                                    print("\n" + "=" * 80)

                            # 显示其他涨停股票
                            if not other_stocks.empty:
                                print("\n=== 其他涨停股票 ===")
                                print("=" * 80)
                                for _, stock in other_stocks.iterrows():
                                    print(f"\n{stock['ts_code']} {stock['name']} "
                                          f"涨幅: {stock['pct_chg']:.2f}% "
                                          f"成交额: {float(stock['amount']) / 10000:.2f}万")

                                    # 显示该股票所属的所有概念（涨停数<3的概念）
                                    try:
                                        concepts = self.pro.concept_detail(ts_code=stock['ts_code'])
                                        if concepts is not None and not concepts.empty:
                                            stock_concepts = []
                                            for _, concept in concepts.iterrows():
                                                concept_name = concept['concept_name']
                                                if concept_name in concept_groups:
                                                    count = len(concept_groups[concept_name])
                                                    stock_concepts.append(f"{concept_name}({count}只涨停)")
                                            if stock_concepts:
                                                print(f"所属概念: {', '.join(stock_concepts)}")
                                    except Exception as e:
                                        print(f"获取概念信息失败: {str(e)}")

                                    print("-" * 80)

                print(f"\n等待 {interval} 秒后开始下一轮...")
                time.sleep(interval)

            except Exception as e:
                print(f"监控异常: {str(e)}")
                time.sleep(interval)

    def get_batch_data(self, batch):
        """获取单个批次的数据"""
        try:
            stock_str = ','.join(batch)
            df = ts.realtime_quote(ts_code=stock_str)
            if df is not None and not df.empty:
                df.columns = df.columns.str.lower()
                return df
        except Exception as e:
            print(f"获取批次数据失败: {str(e)}")
        return None

    def process_stock_data(self, stock_code, period='3days'):
        """处理单个股票数据，添加重试机制"""
        max_retries = 3
        retry_delay = 1  # 初始延迟1秒

        for attempt in range(max_retries):
            try:
                today = datetime.now().strftime('%Y%m%d')
                if period == '3days':
                    # 修改：获取今天之前的3个交易日数据
                    start_date = (datetime.now() - pd.Timedelta(days=7)).strftime('%Y%m%d')
                    df_daily = self.pro.daily(ts_code=stock_code,
                                              start_date=start_date,
                                              end_date=today)

                    if df_daily is not None and not df_daily.empty:
                        # 修改：排除今天的数据，只看之前3个交易日
                        df_daily = df_daily[df_daily['trade_date'] < today]
                        recent_days = df_daily.head(3)  # 取最近3个交易日
                        has_limit_up = (recent_days['pct_chg'] >= 9.5).any()
                else:  # 3months
                    start_date = (datetime.now() - pd.Timedelta(days=90)).strftime('%Y%m%d')
                    df_daily = self.pro.daily(ts_code=stock_code,
                                              start_date=start_date,
                                              end_date=today)

                    if df_daily is not None and not df_daily.empty:
                        has_limit_up = (df_daily['pct_chg'] >= 9.5).any()

                if df_daily is not None and not df_daily.empty:
                    with self.lock:
                        print(f"股票 {stock_code} {period}内{'有' if has_limit_up else '无'}涨停")
                    return stock_code if has_limit_up else None

            except Exception as e:
                if "每分钟最多访问该接口800次" in str(e):
                    retry_delay *= 2  # 指数退避
                    with self.lock:
                        print(f"处理{stock_code}遇到限频，等待{retry_delay}秒后重试...")
                    time.sleep(retry_delay)
                    continue

                with self.lock:
                    print(f"处理{stock_code}失败: {str(e)}")
                break

        return None

    def get_filtered_stocks(self):
        # 获取基础股票信息
        data = self.pro.stock_basic(exchange='', list_status='L',
                                    fields='ts_code,symbol,name,area,industry,list_date')

        # 先进行基本筛选
        filtered_stocks = data[
            (~data['ts_code'].str.startswith('300')) &  # 排除创业板
            (~data['ts_code'].str.startswith('688')) &  # 排除科创板
            (~data['name'].str.contains('ST')) &  # 排除ST股票
            (~data['name'].str.contains('银行|中国|保险|证券|铁路|电信|石油|工商|农业|建设|中信|招商')) &  # 排除大型蓝筹股
            (data['ts_code'].str.endswith(('SH', 'SZ')))  # 只保留沪深主板
            ]

        print(f"\n初步筛选结果:")
        print(f"共筛选出 {len(filtered_stocks)} 只股票")

        # 获取当前日期
        today = datetime.now().strftime('%Y%m%d')

        try:
            # 第一步：并发检查3天内涨停，降低并发数
            print("\n开始并发检查最近3天涨停情况...")
            limit_up_stocks_3days = set()

            # 每批处理的股票数量
            batch_size = 50
            stock_codes = filtered_stocks['ts_code'].tolist()
            total_batches = len(stock_codes) // batch_size + (1 if len(stock_codes) % batch_size else 0)

            # 使用tqdm创建进度条
            with tqdm(total=len(stock_codes), desc="处理3天涨停数据") as pbar:
                for i in range(0, len(stock_codes), batch_size):
                    batch = stock_codes[i:i + batch_size]
                    print(f"\n处理第 {i // batch_size + 1}/{total_batches} 批，共 {len(batch)} 只股票")

                    with ThreadPoolExecutor(max_workers=2) as executor:
                        futures = [executor.submit(self.process_stock_data, code, '3days')
                                   for code in batch]

                        for future in concurrent.futures.as_completed(futures):
                            result = future.result()
                            if result:
                                limit_up_stocks_3days.add(result)
                            pbar.update(1)  # 更新进度条

                    time.sleep(2)

            # 过滤掉3天内涨停的股票
            filtered_stocks = filtered_stocks[~filtered_stocks['ts_code'].isin(limit_up_stocks_3days)]
            print(f"\n3天内涨停过滤完成: {len(limit_up_stocks_3days)} 只股票被过滤")

            # 第二步：并发检查半年内涨停
            print("\n开始并发检查半年内涨停情况...")
            limit_up_stocks_6months = set()
            remaining_stocks = filtered_stocks['ts_code'].tolist()

            # 为半年数据检查创建新的进度条
            with tqdm(total=len(remaining_stocks), desc="处理半年涨停数据") as pbar:
                for i in range(0, len(remaining_stocks), batch_size):
                    batch = remaining_stocks[i:i + batch_size]
                    current_batch = i // batch_size + 1
                    total_batches = len(remaining_stocks) // batch_size + (
                        1 if len(remaining_stocks) % batch_size else 0)
                    print(f"\n处理第 {current_batch}/{total_batches} 批，共 {len(batch)} 只股票")

                    with ThreadPoolExecutor(max_workers=2) as executor:
                        futures = [executor.submit(self.process_stock_data, code, '6months')
                                   for code in batch]

                        for future in concurrent.futures.as_completed(futures):
                            result = future.result()
                            if result:
                                limit_up_stocks_6months.add(result)
                            pbar.update(1)  # 更新进度条

                    time.sleep(2)

            # 只保留半年内有涨停的股票
            filtered_stocks = filtered_stocks[filtered_stocks['ts_code'].isin(limit_up_stocks_6months)]
            print(f"\n半年涨停筛选完成: 保留 {len(filtered_stocks)} 只股票")

            # 获取实时价格数据
            try:
                all_filtered_stocks = []
                batch_size = 100
                # 修改：确保使用正确格式的股票代码
                stock_codes = filtered_stocks['ts_code'].tolist()
                stock_batches = [stock_codes[i:i + batch_size]
                                 for i in range(0, len(stock_codes), batch_size)]

                print(f"共有 {len(stock_batches)} 批股票需要处理...")

                for i, batch in enumerate(stock_batches, 1):
                    if not batch:  # 检查批次是否为空
                        continue

                    print(f"正在处理第 {i}/{len(stock_batches)} 批...")
                    try:
                        # 转换股票代码格式
                        formatted_codes = [code.split('.')[0] for code in batch]
                        price_data = ts.get_realtime_quotes(formatted_codes)

                        if price_data is not None and not price_data.empty:
                            price_data['price'] = price_data['price'].astype(float)
                            # 筛选价格在3元到20元之间的股票
                            price_filtered = price_data[(price_data['price'] > 3) & (price_data['price'] <= 20)]

                            # 将代码转回原格式进行匹配
                            original_codes = [f"{code}.{batch[idx].split('.')[1]}"
                                              for idx, code in enumerate(price_filtered['code'])]
                            batch_filtered = filtered_stocks[filtered_stocks['ts_code'].isin(original_codes)]

                            if not batch_filtered.empty:
                                all_filtered_stocks.append(batch_filtered)
                    except Exception as e:
                        print(f"处理批次 {i} 时出错: {str(e)}")
                        continue

                    time.sleep(0.5)

                if all_filtered_stocks:
                    filtered_stocks = pd.concat(all_filtered_stocks, ignore_index=True)
                else:
                    filtered_stocks = pd.DataFrame()

            except Exception as e:
                print(f"获取实时价格数据失败: {str(e)}")
                filtered_stocks = pd.DataFrame()

        except Exception as e:
            print(f"获取实时价格数据失败: {str(e)}")
            filtered_stocks = pd.DataFrame()

        return filtered_stocks

    def save_filtered_stocks(self, filename='filtered_stocks.csv'):
        """保存筛选后的股票到文件"""
        filtered_stocks = self.get_filtered_stocks()

        if not filtered_stocks.empty:
            # 添加筛选时间
            filtered_stocks['filter_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # 保存到CSV文件
            filtered_stocks.to_csv(filename, index=False)
            print(f"\n筛选结果已保存到 {filename}")
            print(f"共保存 {len(filtered_stocks)} 只股票")
            return True
        return False

    def load_filtered_stocks(self, filename='filtered_stocks.csv'):
        """从文件加载筛选后的股票"""
        try:
            if not os.path.exists(filename):
                print(f"文件 {filename} 不存在，请先运行筛选")
                return False

            filtered_stocks = pd.read_csv(filename)
            filter_time = filtered_stocks['filter_time'].iloc[0]
            print(f"\n加载 {filter_time} 筛选的股票")
            print(f"共加载 {len(filtered_stocks)} 只股票")

            # 更新监控列表
            self.stock_list = filtered_stocks['ts_code'].tolist()
            return True
        except Exception as e:
            print(f"加载文件失败: {str(e)}")
            return False

    def get_industry_info(self):
        """获取所有股票的行业信息"""
        try:
            industry_data = self.pro.stock_basic(fields='ts_code,industry')
            return industry_data.set_index('ts_code')['industry'].to_dict()
        except Exception as e:
            print(f"获取行业信息失败: {str(e)}")
            return {}

    def get_realtime_data_parallel(self, max_workers=5):
        """并行获取所有股票实时数据"""
        # 将股票列表分成多个子列表
        chunk_size = 50  # tushare单次请求限制
        stock_chunks = [self.stock_list[i:i + chunk_size]
                        for i in range(0, len(self.stock_list), chunk_size)]

        all_data = []

        def fetch_chunk(stocks):
            try:
                stock_str = ','.join(stocks)
                return ts.realtime_quote(ts_code=stock_str)
            except Exception as e:
                print(f"获取数据出错: {e}")
                return None

        # 使用线程池并行获取数据
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(fetch_chunk, stock_chunks))

        # 合并所有结果
        return pd.concat([df for df in results if df is not None], ignore_index=True)

    def get_concept_info(self):
        """获取开盘啦题材概念数据"""
        try:
            today = datetime.now().strftime('%Y%m%d')
            concept_data = self.pro.kpl_concept(trade_date=today)
            if concept_data is not None and not concept_data.empty:
                # 创建股票代码到题材的映射字典
                stock_concept_map = {}
                for _, row in concept_data.iterrows():
                    if row['z_t_num'] > 0:  # 只关注有涨停股的题材
                        # 获取该题材下的股票列表
                        concept_stocks = self.pro.concept_detail(
                            ts_code=row['ts_code'],
                            trade_date=today
                        )
                        if concept_stocks is not None and not concept_stocks.empty:
                            for _, stock in concept_stocks.iterrows():
                                if stock['ts_code'] not in stock_concept_map:
                                    stock_concept_map[stock['ts_code']] = []
                                stock_concept_map[stock['ts_code']].append({
                                    'concept_name': row['name'],
                                    'z_t_num': row['z_t_num']
                                })
                return stock_concept_map
        except Exception as e:
            print(f"获取题材概念数据失败: {str(e)}")
        return {}

    def check_first_limit_up(self, ts_code):
        """检查是否3天内首次涨停"""
        try:
            today = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - pd.Timedelta(days=3)).strftime('%Y%m%d')
            df = self.pro.daily(ts_code=ts_code, start_date=start_date, end_date=today)

            if df is not None and not df.empty:
                # 检查最近3天是否有涨停
                recent_limit_up = df[df['pct_chg'] >= 9.5]
                if len(recent_limit_up) == 1 and recent_limit_up.iloc[0]['trade_date'] == today:
                    return True
            return False
        except Exception as e:
            print(f"检查首次涨停失败 {ts_code}: {str(e)}")
            return False

    def check_volume_threshold(self, ts_code):
        """检查10:30前成交量是否超过1.5亿"""
        try:
            current_time = datetime.now().time()
            if current_time > datetime.strptime('10:30:00', '%H:%M:%S').time():
                return False

            df = self.pro.daily(ts_code=ts_code, start_date=datetime.now().strftime('%Y%m%d'))
            if df is not None and not df.empty:
                volume = float(df.iloc[0]['amount'])  # 成交额
                return volume > 150000000  # 1.5亿
            return False
        except Exception as e:
            print(f"检查成交量失败 {ts_code}: {str(e)}")
            return False

    def find_related_stocks(self, limit_up_stocks, all_stocks_df):
        """根据涨跌幅相关性找出关联股票"""
        related_groups = {}

        for stock in limit_up_stocks:
            if stock not in self.related_stocks:
                self.related_stocks[stock] = set()

            # 获取该股票近期的涨跌数据
            stock_data = self.get_recent_price_changes(stock)
            if stock_data is None:
                continue

            # 查找涨跌幅相关的股票
            for _, row in all_stocks_df.iterrows():
                other_stock = row['ts_code']
                if other_stock == stock:
                    continue

                other_data = self.get_recent_price_changes(other_stock)
                if other_data is None:
                    continue

                # 计算相关性
                if self.check_price_correlation(stock_data, other_data):
                    self.related_stocks[stock].add(other_stock)

            # 根据关联股票数量分组
            group_key = len(self.related_stocks[stock])
            if group_key not in related_groups:
                related_groups[group_key] = set()
            related_groups[group_key].add(stock)

        return related_groups

    def get_recent_price_changes(self, ts_code, days=5):
        """获取股票最近几天的涨跌数据"""
        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - pd.Timedelta(days=days)).strftime('%Y%m%d')
            df = self.pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if df is not None and not df.empty:
                return df['pct_chg'].tolist()
            return None
        except Exception as e:
            print(f"获取{ts_code}历史数据失败: {str(e)}")
            return None

    def check_price_correlation(self, data1, data2, threshold=0.8):
        """检查两只股票的涨跌相关性"""
        try:
            if len(data1) != len(data2):
                return False
            correlation = np.corrcoef(data1, data2)[0, 1]
            return correlation >= threshold
        except Exception:
            return False

    def send_feishu_message(self, concept_name, stocks, potential_stocks):
        """发送飞书通知"""
        try:
            # 构建消息内容
            message = f"🔥 热门板块提醒 🔥\n\n"
            message += f"【{concept_name}】概念 已有{len(stocks)}只涨停\n\n"

            message += "涨停股票:\n"
            for stock in stocks:
                message += (f"- {stock['name']}({stock['ts_code']}) "
                            f"涨幅: {stock['pct_chg']:.2f}% "
                            f"成交额: {float(stock['amount']) / 10000:.2f}万\n")

            if not potential_stocks.empty:
                message += "\n潜力股票:\n"
                for _, stock in potential_stocks.iterrows():
                    message += (f"- {stock['name']}({stock['ts_code']}) "
                                f"涨幅: {stock['pct_chg']:.2f}% "
                                f"成交额: {float(stock['amount']) / 10000:.2f}万\n")

            # 发送消息
            data = {
                "msg_type": "text",
                "content": {
                    "text": message
                }
            }
            response = requests.post(self.feishu_webhook, json=data)
            if response.status_code != 200:
                print(f"发送飞书消息失败: {response.text}")

        except Exception as e:
            print(f"发送飞书消息失败: {str(e)}")

    def filter_stocks(self):
        """筛选股票
        条件:
        1. 今天涨停
        2. 昨天和前天都没有涨停
        即：找出真正的首板股票
        """
        today = datetime.now()
        today_str = today.strftime('%Y%m%d')
        yesterday = (today - timedelta(days=1))
        yesterday_str = yesterday.strftime('%Y%m%d')
        day_before = (today - timedelta(days=2))
        day_before_str = day_before.strftime('%Y%m%d')

        try:
            # 获取最近3天的数据
            df = self.pro.daily(start_date=day_before_str,
                                end_date=today_str)

            # 按日期分组
            today_df = df[df['trade_date'] == today_str]
            yesterday_df = df[df['trade_date'] == yesterday_str]
            day_before_df = df[df['trade_date'] == day_before_str]

            # 1. 找出今天涨停的股票
            today_limit_up = set(today_df[today_df['pct_chg'] >= 9.5]['ts_code'])

            # 2. 找出昨天和前天涨停的股票
            yesterday_limit_up = set(yesterday_df[yesterday_df['pct_chg'] >= 9.5]['ts_code'])
            day_before_limit_up = set(day_before_df[day_before_df['pct_chg'] >= 9.5]['ts_code'])

            # 3. 筛选真正的首板：今天涨停，昨天和前天都没涨停
            first_limit_up = today_limit_up - yesterday_limit_up - day_before_limit_up

            # 4. 打印详细信息
            print(f"今日涨停数量: {len(today_limit_up)}")
            print(f"昨日涨停数量: {len(yesterday_limit_up)}")
            print(f"前日涨停数量: {len(day_before_limit_up)}")
            print(f"真正首板数量: {len(first_limit_up)}")

            # 5. 获取首板股票的详细信息
            first_limit_details = []
            for stock in first_limit_up:
                stock_data = today_df[today_df['ts_code'] == stock].iloc[0]
                first_limit_details.append({
                    'ts_code': stock,
                    'name': self.get_stock_name(stock),
                    'price': stock_data['close'],
                    'pct_chg': stock_data['pct_chg'],
                    'amount': stock_data['amount'] / 10000  # 转换为万元
                })

            # 6. 按成交额排序
            first_limit_details.sort(key=lambda x: x['amount'], reverse=True)

            # 7. 打印首板详情
            print("\n今日首板股票详情:")
            for stock in first_limit_details:
                print(f"股票: {stock['name']}({stock['ts_code']}) "
                      f"价格: {stock['price']} "
                      f"涨幅: {stock['pct_chg']:.2f}% "
                      f"成交额: {stock['amount']:.2f}万")

            return [stock['ts_code'] for stock in first_limit_details]

        except Exception as e:
            print(f"筛选股票失败: {str(e)}")
            return []

    def get_stock_name(self, ts_code):
        """获取股票名称"""
        try:
            df = self.pro.stock_basic(ts_code=ts_code, fields='name')
            return df.iloc[0]['name']
        except:
            return ts_code

    def wait_for_market_open(self):
        """等待市场开盘"""
        while True:
            now = datetime.now()
            market_open = datetime.strptime(f"{now.strftime('%Y-%m-%d')} 09:30:00", '%Y-%m-%d %H:%M:%S')

            if now < market_open:
                wait_seconds = (market_open - now).total_seconds()
                if wait_seconds > 60:  # 如果等待时间超过1分钟
                    print(f"\r等待开盘，还有 {int(wait_seconds / 60)} 分钟...", end='')
                    time.sleep(60)
                else:
                    print(f"\r等待开盘，还有 {int(wait_seconds)} 秒...", end='')
                    time.sleep(1)
            else:
                print("\n市场已开盘，开始运行程序...")
                break


if __name__ == "__main__":
    monitor = StockMonitor([])

    if len(sys.argv) > 1 and sys.argv[1] == 'filter':
        # 运行筛选并保存
        monitor.save_filtered_stocks()
    else:
        # 等待开盘
        monitor.wait_for_market_open()

        # 从文件加载并监控
        if monitor.load_filtered_stocks():
            try:
                monitor.monitor(interval=1)
            except KeyboardInterrupt:
                print("\n程序已停止")
        else:
            print("加载股票列表失败，请先运行筛选")