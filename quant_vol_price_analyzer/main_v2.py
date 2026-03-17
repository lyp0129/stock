#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量价关系分析工具 v2.1 (Quant Vol-Price Analyzer)
专业版 - 趋势过滤 + 分批止盈 + ATR止损

核心升级：
1. ✅ 趋势过滤（MA20/MA60）
2. ✅ 分批止盈（20/60/120日）
3. ✅ ATR动态止损
4. ✅ 支撑位优化（60日）
5. ✅ 避免追高判断
6. ✅ 模块化结构
"""

import tushare as ts
import pandas as pd
import numpy as np
import argparse
import os
import yaml
from datetime import datetime, timedelta
from typing import Tuple, Dict, Optional, List
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed


class TrendType(Enum):
    """趋势类型"""
    UPTREND = "上升趋势"
    DOWNTREND = "下降趋势"
    RANGE = "震荡整理"


class VolPriceAnalyzer:
    """量价关系分析器 v2.1 - 专业版"""

    # 量价关系配置
    VOL_PRICE_CONFIG = {
        '1A': {'name': '震荡整理', 'action': '观望', 'action_code': '①', 'desc': '多空平衡，市场方向不明，等待突破信号'},
        '1B': {'name': '温和上涨', 'action': '观望/持仓', 'action_code': '①', 'desc': '资金试探性拉升，趋势初期常见，不宜追高'},
        '1C': {'name': '阴跌走势', 'action': '减仓', 'action_code': '③', 'desc': '价格缓慢下行，资金承接弱，可能持续阴跌'},
        '2A': {'name': '放量换手', 'action': '观望/高位减仓', 'action_code': '①', 'desc': '低位可能是换手蓄势；高位需警惕主力出货'},
        '2B': {'name': '量价齐升', 'action': '买入/持仓', 'action_code': '②', 'desc': '最健康上涨结构，资金持续流入，趋势行情'},
        '2C': {'name': '放量下跌', 'action': '卖出', 'action_code': '③', 'desc': '资金集中出逃，趋势转弱，止损信号'},
        '3A': {'name': '缩量整理', 'action': '观望', 'action_code': '①', 'desc': '市场等待方向，常出现在变盘前'},
        '3B': {'name': '缩量上涨', 'action': '观望/持仓', 'action_code': '①', 'desc': '抛压较轻，主力控盘迹象，高位需警惕背离'},
        '3C': {'name': '缩量下跌', 'action': '空仓/观察', 'action_code': '④', 'desc': '卖盘衰竭，可能接近底部，但需等待止跌信号'},
    }

    def __init__(self, token: str = None, config_path: str = None, proxy_url: str = 'http://lianghua.nanyangqiankun.top'):
        """初始化分析器

        Args:
            token: Tushare API token
            config_path: 配置文件路径
            proxy_url: API代理地址
        """
        if token:
            ts.set_token(token)
        else:
            # 尝试从配置文件获取 token
            token = self._load_token_from_config(config_path)

            if not token:
                # 尝试从环境变量获取
                token = os.environ.get('TUSHARE_TOKEN', '')

            if not token:
                raise ValueError("Tushare Token 未配置")

            ts.set_token(token)

        self.pro = ts.pro_api()
        # 设置必要的属性
        self.pro._DataApi__token = token
        self.pro._DataApi__http_url = proxy_url

        # 缓存股票基本信息
        self.stock_map = {}
        self._init_stock_cache()

    def _load_token_from_config(self, config_file):
        """从配置文件加载 token"""
        try:
            if config_file and os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                    return config.get('tushare_token', '')
        except Exception as e:
            print(f"警告: 读取配置文件失败 ({e})")
        return ''

    def _init_stock_cache(self):
        """初始化股票基本信息缓存"""
        try:
            print("正在加载股票基本信息...")
            stock_basic = self.pro.stock_basic(fields='ts_code,name')
            self.stock_map = dict(zip(stock_basic.ts_code, stock_basic.name))
            print(f"已加载 {len(self.stock_map)} 只股票信息")
        except Exception as e:
            print(f"警告: 加载股票信息失败 ({e})")
            self.stock_map = {}

    def get_stock_name(self, ts_code: str) -> str:
        """获取股票名称

        Args:
            ts_code: 股票代码

        Returns:
            股票名称
        """
        return self.stock_map.get(ts_code, ts_code)

    def get_stock_data(self, ts_code: str, days: int = 60) -> pd.DataFrame:
        """获取股票历史数据

        Args:
            ts_code: 股票代码（如 000001.SZ）
            days: 获取最近多少天的数据（默认 60 天）

        Returns:
            包含股票数据的 DataFrame
        """
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=days+30)).strftime('%Y%m%d')

        # 获取日线数据
        df = self.pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)

        if df.empty:
            raise ValueError(f"未获取到股票 {ts_code} 的数据，请检查股票代码")

        # 按日期排序，取最近 days 天
        df = df.sort_values('trade_date').tail(days)

        return df

    def analyze_volume_status(self, df: pd.DataFrame) -> Tuple[str, float]:
        """分析成交量状态（优化版：使用 10 日量比）

        Args:
            df: 股票数据 DataFrame

        Returns:
            ('量平'、'量升' 或 '量缩', 量比)
        """
        if len(df) < 10:
            return '量平', 1.0

        # 最近一天的成交量
        recent_vol = df.iloc[-1]['vol']
        # 前10天平均成交量
        avg_vol_10 = df.iloc[-11:-1]['vol'].mean()

        if avg_vol_10 == 0:
            return '量平', 1.0

        # 计算量比
        vol_ratio = recent_vol / avg_vol_10

        # 判断成交量状态（更稳健的阈值）
        if vol_ratio > 1.5:
            return '量升', vol_ratio
        elif vol_ratio < 0.7:
            return '量缩', vol_ratio
        else:
            return '量平', vol_ratio

    def analyze_price_status(self, df: pd.DataFrame) -> str:
        """分析价格状态（优化版：使用 1% 阈值）

        Args:
            df: 股票数据 DataFrame

        Returns:
            '价平'、'价涨' 或 '价跌'
        """
        if len(df) < 2:
            return '价平'

        # 最近两天的收盘价
        recent_close = df.iloc[-1]['close']
        prev_close = df.iloc[-2]['close']

        # 计算涨跌幅
        change_pct = (recent_close - prev_close) / prev_close

        # 判断价格状态（使用 1% 阈值，减少噪音）
        if change_pct > 0.01:
            return '价涨'
        elif change_pct < -0.01:
            return '价跌'
        else:
            return '价平'

    def get_vol_price_pattern(self, vol_status: str, price_status: str) -> str:
        """获取量价关系形态

        Args:
            vol_status: 成交量状态（'量平'、'量升'、'量缩'）
            price_status: 价格状态（'价平'、'价涨'、'价跌'）

        Returns:
            量价关系代码（如 '2B'）
        """
        vol_map = {'量平': '1', '量升': '2', '量缩': '3'}
        price_map = {'价平': 'A', '价涨': 'B', '价跌': 'C'}

        return vol_map[vol_status] + price_map[price_status]

    def analyze_position(self, df: pd.DataFrame) -> str:
        """判断股票位置（优化版：使用 120 日区间）

        Args:
            df: 股票数据 DataFrame

        Returns:
            '高位'、'中位' 或 '低位'
        """
        if len(df) < 120:
            # 如果数据不足，使用 20 日区间
            recent = df.tail(20)
        else:
            recent = df.tail(120)

        max_price = recent['high'].max()
        min_price = recent['low'].min()
        current_price = df.iloc[-1]['close']

        # 计算当前价格在区间的位置
        price_range = max_price - min_price
        if price_range == 0:
            return '中位'

        position = (current_price - min_price) / price_range

        # 更严格的阈值（80% 和 20%）
        if position > 0.8:
            return '高位'
        elif position < 0.2:
            return '低位'
        else:
            return '中位'

    def analyze_trend(self, df: pd.DataFrame) -> TrendType:
        """分析股票趋势（MA20/MA60/价格）

        Args:
            df: 股票数据 DataFrame

        Returns:
            TrendType 枚举值
        """
        if len(df) < 60:
            return TrendType.RANGE

        ma20 = df['close'].rolling(20).mean().iloc[-1]
        ma60 = df['close'].rolling(60).mean().iloc[-1]
        price = df.iloc[-1]['close']

        # 上升趋势：价格 > MA20 > MA60
        if price > ma20 > ma60:
            return TrendType.UPTREND
        # 下降趋势：价格 < MA20 < MA60
        elif price < ma20 < ma60:
            return TrendType.DOWNTREND
        # 震荡整理
        else:
            return TrendType.RANGE

    def is_chasing_high(self, df: pd.DataFrame) -> bool:
        """判断是否追高（价格接近20日阻力位）

        Args:
            df: 股票数据 DataFrame

        Returns:
            True 表示追高，False 表示安全
        """
        if len(df) < 20:
            return False

        recent_20 = df.tail(20)
        resistance_20 = recent_20['high'].max()
        current_price = df.iloc[-1]['close']

        # 如果当前价格 > 20日阻力位的95%，视为追高
        return current_price > resistance_20 * 0.95

    def calculate_target_prices(self, df: pd.DataFrame, position: str,
                               action_code: int, pattern: str = '', trend: TrendType = TrendType.RANGE) -> Tuple[float, float, float, float, float, float, float, float]:
        """计算目标买入价、卖出价和止损价（专业版：分批止盈策略）

        策略说明：
        - 第一目标：20日阻力位（安全止盈，卖50%）
        - 第二目标：60日阻力位（趋势止盈，卖30%）
        - 第三目标：120日阻力位或突破后15%（主升浪，卖20%）

        v2.1 升级：
        - 支撑位使用60日低点（更可靠）
        - 根据趋势调整止损策略

        Args:
            df: 股票数据 DataFrame
            position: 位置（'高位'、'中位'、'低位'）
            action_code: 操作代码（1=观望, 2=买入, 3=卖出, 4=空仓）
            pattern: 量价形态（用于判断是否强势）
            trend: 趋势类型

        Returns:
            (买入价, 止损价, 目标价1, 目标价2, 目标价3, 阻力位, 支撑位, ATR)
        """
        # 计算多周期阻力位和支撑位
        recent_20 = df.tail(20)
        recent_60 = df.tail(60)
        recent_120 = df.tail(120) if len(df) >= 120 else df.tail(len(df))

        resistance_20 = recent_20['high'].max()
        resistance_60 = recent_60['high'].max()
        resistance_120 = recent_120['high'].max()

        # v2.1 升级：支撑位使用60日低点（更可靠）
        support = recent_60['low'].min()
        current_price = df.iloc[-1]['close']

        # 计算ATR（用于动态止损）
        atr = self._calculate_atr(df, 14)

        # 根据形态调整目标价
        if pattern == '2B':  # 量价齐升，强势形态
            # 强势股给予更大空间
            target1 = resistance_20 * 1.03   # 第一目标：突破20日高点3%
            target2 = resistance_60 * 1.05   # 第二目标：突破60日高点5%
            target3 = resistance_120 * 1.08  # 第三目标：突破120日高点8%
        elif pattern in ['1B', '3B']:  # 温和上涨
            target1 = resistance_20 * 1.02
            target2 = resistance_60 * 1.03
            target3 = resistance_120 * 1.05
        elif pattern in ['1C', '2C', '3C']:  # 下跌形态，给予保守目标
            target1 = resistance_20 * 1.01
            target2 = resistance_60 * 1.02
            target3 = resistance_120 * 1.03
        else:  # 其他形态（1A震荡、2A放量换手、3A缩量整理）
            target1 = resistance_20 * 1.015
            target2 = resistance_60 * 1.025
            target3 = resistance_120 * 1.04

        # 根据操作和位置调整止盈止损
        if action_code == 2:  # 买入
            buy_price = current_price
            # v2.1 升级：根据趋势调整止损
            if trend == TrendType.UPTREND:
                # 上升趋势：宽松止损（ATR*2.5）
                stop_loss = max(support * 0.97, current_price - atr * 2.5)
            elif trend == TrendType.DOWNTREND:
                # 下降趋势：严格止损（ATR*1.5）
                stop_loss = max(support * 0.98, current_price - atr * 1.5)
            else:
                # 震荡：标准止损（ATR*2）
                stop_loss = max(support * 0.97, current_price - atr * 2)
            # 返回第一个目标价
            target = target1
        elif action_code == 3:  # 卖出/减仓（持有状态）
            buy_price = current_price
            stop_loss = max(support * 0.97, current_price * 0.95, current_price - atr * 2)
            target = target1  # 第一目标价
        else:  # 观望/空仓
            if position == '低位':
                buy_price = support * 1.02
                stop_loss = support * 0.95
                target = target1
            elif position == '高位':
                buy_price = current_price * 0.98
                stop_loss = support * 0.95
                target = target1
            else:  # 中位
                buy_price = current_price
                stop_loss = support * 0.97
                target = target1

        return (round(buy_price, 2), round(stop_loss, 2), round(target1, 2),
                round(target2, 2), round(target3, 2), round(resistance_20, 2),
                round(support, 2), round(atr, 2))

    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """计算ATR（平均真实波幅）

        Args:
            df: 股票数据 DataFrame
            period: 周期

        Returns:
            ATR值
        """
        if len(df) < period + 1:
            return df['close'].iloc[-1] * 0.02  # 默认2%

        # 计算真实波幅
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())

        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

        # ATR = TR的移动平均
        atr = tr.tail(period).mean()

        return atr

    def analyze(self, ts_code: str, shares: int = 0, cost: float = 0.0) -> Dict:
        """分析股票量价关系（v2.1 升级版：集成趋势过滤）

        Args:
            ts_code: 股票代码（如 000001.SZ）
            shares: 持有股数
            cost: 成本价

        Returns:
            分析结果字典
        """
        # 获取股票数据
        df = self.get_stock_data(ts_code)

        # 获取基本信息
        current_price = df.iloc[-1]['close']
        prev_price = df.iloc[-2]['close'] if len(df) > 1 else current_price
        change_pct = (current_price - prev_price) / prev_price * 100

        # 获取股票名称（从缓存）
        stock_name = self.get_stock_name(ts_code)

        # 分析量价关系
        vol_status, vol_ratio = self.analyze_volume_status(df)
        price_status = self.analyze_price_status(df)
        pattern = self.get_vol_price_pattern(vol_status, price_status)
        position = self.analyze_position(df)

        # v2.1 升级：分析趋势
        trend = self.analyze_trend(df)

        # v2.1 升级：检查是否追高
        is_chasing = self.is_chasing_high(df)

        # 获取配置
        config = self.VOL_PRICE_CONFIG[pattern]

        # 解析操作代码
        action_map = {'①': 1, '②': 2, '③': 3, '④': 4}
        action_code = action_map[config['action_code']]

        # v2.1 升级：根据趋势调整操作建议
        if trend == TrendType.DOWNTREND and action_code == 2:
            # 下降趋势中，不建议买入
            action = '观望（下降趋势）'
            action_code = 1
        elif is_chasing and action_code == 2:
            # 追高状态，不建议买入
            action = '观望（价格接近阻力位）'
            action_code = 1
        elif shares > 0 and cost > 0:
            # 如果是持仓状态，调整操作建议
            if pattern in ['1B', '2B', '3B']:  # 可以继续持仓的情况
                action = '持仓'
                action_code = 1
            elif pattern in ['1C', '2A', '2C']:  # 建议减仓的情况
                action = '减仓'
                action_code = 3
            else:
                action = config['action']
        else:
            action = config['action']

        # 计算目标价格（使用分批止盈策略，传入趋势参数）
        buy_price, stop_loss, target1, target2, target3, resistance, support, atr = self.calculate_target_prices(
            df, position, action_code, pattern, trend
        )

        # 计算持仓盈亏
        profit_loss = None
        profit_loss_pct = None
        market_value = None
        if shares > 0 and cost > 0:
            market_value = shares * current_price
            profit_loss = market_value - shares * cost
            profit_loss_pct = (current_price - cost) / cost * 100

        return {
            'ts_code': ts_code,
            'stock_name': stock_name,
            'current_price': current_price,
            'change_pct': change_pct,
            'vol_status': vol_status,
            'price_status': price_status,
            'vol_ratio': vol_ratio,
            'pattern': pattern,
            'pattern_name': config['name'],
            'action': action,
            'action_code': config['action_code'],
            'description': config['desc'],
            'position': position,
            'trend': trend.value,  # v2.1 升级
            'is_chasing': is_chasing,  # v2.1 升级
            'buy_price': buy_price,
            'stop_loss_price': stop_loss,
            'target_price1': target1,  # 第一目标（20日）
            'target_price2': target2,  # 第二目标（60日）
            'target_price3': target3,  # 第三目标（120日）
            'resistance': resistance,
            'support': support,
            'atr': atr,
            'shares': shares,
            'cost': cost,
            'market_value': market_value,
            'profit_loss': profit_loss,
            'profit_loss_pct': profit_loss_pct,
        }

    def print_report(self, result: Dict):
        """打印分析报告（v2.1 升级版）

        Args:
            result: 分析结果字典
        """
        print("\n" + "=" * 50)
        print("股票分析报告 v2.1 - 专业版")
        print("=" * 50)
        print(f"股票代码: {result['ts_code']}")
        print(f"股票名称: {result['stock_name']}")
        print(f"当前价格: {result['current_price']:.2f}")

        change_symbol = '+' if result['change_pct'] >= 0 else ''
        print(f"涨跌幅: {change_symbol}{result['change_pct']:.2f}%")

        print("-" * 50)
        print("量价关系分析:")
        print("-" * 50)
        print(f"成交量状态: {result['vol_status']} (量比: {result['vol_ratio']:.2f})")
        print(f"价格状态: {result['price_status']}")
        print(f"量价形态: {result['pattern']} - {result['pattern_name']}")
        print(f"所处位置: {result['position']}")

        # v2.1 升级：显示趋势和追高判断
        print("-" * 50)
        print("趋势分析 (v2.1 新增):")
        print("-" * 50)
        print(f"趋势状态: {result['trend']}")

        trend_emoji = {
            "上升趋势": "📈",
            "下降趋势": "📉",
            "震荡整理": "↔️"
        }
        print(f"  {trend_emoji.get(result['trend'], '')} {result['trend']}")

        if result['is_chasing']:
            print(f"⚠️  追高警告: 当前价格接近20日阻力位，不建议追高买入")
        else:
            print(f"✓ 价格位置: 安全（未接近阻力位）")

        print("-" * 50)
        print("操作建议:")
        print("-" * 50)
        print(f"建议操作: {result['action_code']} {result['action']}")
        print(f"说明: {result['description']}")

        print("-" * 50)
        print("价格分析:")
        print("-" * 50)
        print(f"20日阻力位: {result['resistance']:.2f} 元")
        print(f"60日支撑位: {result['support']:.2f} 元 (v2.1 升级)")
        print(f"ATR波动率: {result['atr']:.2f} 元")

        if result['shares'] > 0 and result['cost'] > 0:
            print("-" * 50)
            print("持仓分析:")
            print("-" * 50)
            print(f"持仓数量: {result['shares']} 股")
            print(f"成本价格: {result['cost']:.2f} 元")
            print(f"当前市值: {result['market_value']:.2f} 元")

            pl_symbol = '+' if result['profit_loss'] >= 0 else ''
            print(f"盈亏情况: {pl_symbol}{result['profit_loss']:.2f} 元 "
                  f"({pl_symbol}{result['profit_loss_pct']:.2f}%)")

            print("\n" + "-" * 50)
            print("分批止盈策略 (按剩余仓位分批，100股整数):")
            print("-" * 50)

            # 计算实际卖出股数（100股整数）
            shares = result['shares']
            sell1 = (shares * 0.5) // 100 * 100  # 第一目标：卖50%
            remain1 = shares - sell1
            sell2 = (remain1 * 0.3) // 100 * 100  # 第二目标：卖剩余30%
            remain2 = remain1 - sell2
            sell3 = (remain2 * 0.2) // 100 * 100  # 第三目标：卖剩余20%
            remain3 = remain2 - sell3

            print(f"第一目标: {result['target_price1']:.2f} 元 (卖{sell1}股 → 剩{remain1}股)")
            print(f"第二目标: {result['target_price2']:.2f} 元 (卖{sell2}股 → 剩{remain2}股)")
            print(f"第三目标: {result['target_price3']:.2f} 元 (卖{sell3}股 → 剩{remain3}股底仓)")
            print(f"说明: 主升浪开启时仍保留{remain3}股底仓，避免错过大行情")

            # v2.1 升级：根据趋势显示止损说明
            if result['trend'] == "上升趋势":
                print(f"建议止损: {result['stop_loss_price']:.2f} 元 (ATR*2.5 - 宽松)")
            elif result['trend'] == "下降趋势":
                print(f"建议止损: {result['stop_loss_price']:.2f} 元 (ATR*1.5 - 严格)")
            else:
                print(f"建议止损: {result['stop_loss_price']:.2f} 元 (ATR*2 - 标准)")
        else:
            print("-" * 50)
            print("分批止盈策略 (按剩余仓位分批):")
            print("-" * 50)
            print(f"第一目标: {result['target_price1']:.2f} 元 (卖剩余50% → 剩50%仓)")
            print(f"第二目标: {result['target_price2']:.2f} 元 (卖剩余30% → 剩35%仓)")
            print(f"第三目标: {result['target_price3']:.2f} 元 (卖剩余20% → 剩28%底仓)")
            print(f"说明: 主升浪开启时仍保留28%底仓，避免错过大行情")

            # v2.1 升级：根据趋势显示止损说明
            if result['trend'] == "上升趋势":
                print(f"建议止损: {result['stop_loss_price']:.2f} 元 (ATR*2.5 - 宽松)")
            elif result['trend'] == "下降趋势":
                print(f"建议止损: {result['stop_loss_price']:.2f} 元 (ATR*1.5 - 严格)")
            else:
                print(f"建议止损: {result['stop_loss_price']:.2f} 元 (ATR*2 - 标准)")

        print("=" * 50 + "\n")

    def scan_market(self, pattern: str = '2B', min_vol_ratio: float = 1.2,
                   exclude_st: bool = True) -> List[Dict]:
        """扫描市场，查找特定量价形态的股票

        Args:
            pattern: 目标形态（如 '2B'）
            min_vol_ratio: 最小量比
            exclude_st: 是否排除 ST 股票

        Returns:
            符合条件的股票列表
        """
        print(f"\n开始扫描市场，寻找 {pattern} 形态股票...")

        # 获取所有股票列表
        stock_list = self.pro.stock_basic(
            exchange='',
            list_status='L',
            fields='ts_code,name'
        )

        # 过滤 ST 股票
        if exclude_st:
            stock_list = stock_list[~stock_list['name'].str.contains('ST', na=False)]

        # 只保留沪深主板
        stock_list = stock_list[stock_list['ts_code'].str.endswith(('SH', 'SZ'))]

        codes = stock_list['ts_code'].tolist()
        print(f"待扫描股票数量: {len(codes)}")

        results = []

        # 使用线程池并发分析
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self._analyze_single, code): code for code in codes}

            for i, future in enumerate(as_completed(futures), 1):
                if i % 100 == 0:
                    print(f"扫描进度: {i}/{len(codes)}")

                try:
                    result = future.result(timeout=10)
                    if result and result['pattern'] == pattern:
                        if result['vol_ratio'] >= min_vol_ratio:
                            results.append(result)
                            print(f"  发现: {result['ts_code']} {result['stock_name']} - 量比 {result['vol_ratio']:.2f}")
                except Exception as e:
                    pass

        # 按量比排序
        results.sort(key=lambda x: x['vol_ratio'], reverse=True)

        print(f"\n扫描完成！发现 {len(results)} 只符合条件的股票")
        return results

    def _analyze_single(self, ts_code: str) -> Optional[Dict]:
        """分析单只股票（用于并发扫描）

        Args:
            ts_code: 股票代码

        Returns:
            分析结果字典
        """
        try:
            return self.analyze(ts_code)
        except:
            return None

    def batch_analyze(self, codes: List[str]) -> List[Dict]:
        """批量分析股票

        Args:
            codes: 股票代码列表

        Returns:
            分析结果列表
        """
        print(f"\n批量分析 {len(codes)} 只股票...")

        results = []

        for code in codes:
            try:
                result = self.analyze(code)
                results.append(result)
                print(f"  ✓ {code} - {result['pattern']}")
            except Exception as e:
                print(f"  ✗ {code} - {e}")

        return results


def format_stock_code(code: str) -> str:
    """格式化股票代码

    Args:
        code: 股票代码（如 000001 或 000001.SZ）

    Returns:
        格式化后的股票代码（如 000001.SZ）
    """
    if '.' in code:
        return code.upper()

    # 根据代码前缀判断后缀
    if code.startswith('6'):
        return f"{code}.SH"  # 上海
    elif code.startswith(('0', '3')):
        return f"{code}.SZ"  # 深圳
    elif code.startswith('8') or code.startswith('4'):
        return f"{code}.BJ"  # 北京
    else:
        return code


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='量价关系分析工具 v2.0')
    parser.add_argument('--code', type=str, help='股票代码（如 000001 或 000001,600519）')
    parser.add_argument('--shares', type=int, default=0, help='持有股数（可选）')
    parser.add_argument('--cost', type=float, default=0.0, help='成本价（可选）')
    parser.add_argument('--token', type=str, default=None, help='Tushare Token（可选）')
    parser.add_argument('--config', type=str, default=None, help='配置文件路径（可选）')
    parser.add_argument('--scan', type=str, help='扫描市场，指定目标形态（如 2B）')
    parser.add_argument('--top', type=int, default=10, help='扫描结果显示前 N 名（默认 10）')

    args = parser.parse_args()

    # 默认配置文件路径
    if args.config is None:
        # 获取脚本所在目录的父目录（stock 目录）
        script_dir = os.path.dirname(os.path.abspath(__file__))
        stock_dir = os.path.dirname(script_dir)
        args.config = os.path.join(stock_dir, 'config.prod.yaml')

    try:
        # 创建分析器
        analyzer = VolPriceAnalyzer(token=args.token, config_path=args.config)

        # 市场扫描模式
        if args.scan:
            results = analyzer.scan_market(pattern=args.scan)

            if results:
                print("\n" + "=" * 100)
                print(f"扫描结果 Top {min(args.top, len(results))}")
                print("=" * 100)

                for i, r in enumerate(results[:args.top], 1):
                    print(f"\n{i}. {r['ts_code']} - {r['stock_name']}")
                    print(f"   量价形态: {r['pattern']} ({r['pattern_name']})")
                    print(f"   当前价格: {r['current_price']:.2f} | 量比: {r['vol_ratio']:.2f}")
                    print(f"   位置: {r['position']} | 操作: {r['action_code']} {r['action']}")
                    print(f"   支撑位: {r['support']:.2f} | 阻力位: {r['resistance']:.2f}")

            return 0

        # 单股/批量分析模式
        if args.code:
            # 支持批量分析
            codes = [format_stock_code(c.strip()) for c in args.code.split(',')]

            if len(codes) == 1:
                # 单股分析
                result = analyzer.analyze(codes[0], shares=args.shares, cost=args.cost)
                analyzer.print_report(result)
            else:
                # 批量分析
                results = analyzer.batch_analyze(codes)

                print("\n" + "=" * 100)
                print("批量分析结果")
                print("=" * 100)

                for r in results:
                    print(f"{r['ts_code']} - {r['stock_name']} | "
                          f"{r['pattern']} {r['pattern_name']} | "
                          f"价格: {r['current_price']:.2f} | "
                          f"量比: {r['vol_ratio']:.2f}")
        else:
            parser.print_help()

    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == '__main__':
    exit(main())
