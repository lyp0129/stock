#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票分析 Web 服务
支持手机访问，输入股票代码、持股数、成本价进行分析
"""

from flask import Flask, render_template, request, jsonify
import sys
import os

# 添加当前目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main_v2 import VolPriceAnalyzer, format_stock_code

app = Flask(__name__)

# 全局分析器实例
analyzer = None


def get_analyzer():
    """获取分析器实例（单例模式）"""
    global analyzer
    if analyzer is None:
        # 自动查找配置文件
        config_paths = [
            'config.prod.yaml',
            '../config.prod.yaml',
            os.path.join(os.path.dirname(__file__), '..', 'config.prod.yaml')
        ]
        config_path = None
        for path in config_paths:
            if os.path.exists(path):
                config_path = path
                break

        analyzer = VolPriceAnalyzer(config_path=config_path)
    return analyzer


@app.route('/')
def index():
    """主页 - 显示分析表单"""
    return render_template('index.html')


@app.route('/api/analyze', methods=['POST'])
def api_analyze():
    """分析股票 API"""
    try:
        # 获取参数
        code = request.form.get('code', '').strip()
        shares = request.form.get('shares', '0').strip()
        cost = request.form.get('cost', '0').strip()

        # 验证参数
        if not code:
            return jsonify({'success': False, 'error': '请输入股票代码'})

        try:
            shares = int(shares) if shares else 0
            cost = float(cost) if cost else 0.0
        except ValueError:
            return jsonify({'success': False, 'error': '持股数和成本价格式错误'})

        # 格式化股票代码
        code = format_stock_code(code)

        # 执行分析
        analyzer_instance = get_analyzer()
        result = analyzer_instance.analyze(code, shares=shares, cost=cost)

        # 格式化结果用于显示
        response = {
            'success': True,
            'data': {
                'ts_code': result['ts_code'],
                'stock_name': result['stock_name'],
                'current_price': f"{result['current_price']:.2f}",
                'change_pct': f"{result['change_pct']:+.2f}%",
                'pattern': f"{result['pattern']} - {result['pattern_name']}",
                'action': f"{result['action_code']} {result['action']}",
                'description': result['description'],
                'position': result['position'],
                'trend': result['trend'],
                'is_chasing': result['is_chasing'],
                'vol_ratio': f"{result['vol_ratio']:.2f}",
                'resistance': f"{result['resistance']:.2f}",
                'support': f"{result['support']:.2f}",
                'atr': f"{result['atr']:.2f}",
                'target1': f"{result['target_price1']:.2f}",
                'target2': f"{result['target_price2']:.2f}",
                'target3': f"{result['target_price3']:.2f}",
                'stop_loss': f"{result['stop_loss_price']:.2f}",
            }
        }

        # 如果有持仓信息，添加持仓数据
        if result['shares'] > 0 and result['cost'] > 0:
            response['data']['holding'] = {
                'shares': result['shares'],
                'cost': f"{result['cost']:.2f}",
                'market_value': f"{result['market_value']:.2f}",
                'profit_loss': f"{result['profit_loss']:+.2f}",
                'profit_loss_pct': f"{result['profit_loss_pct']:+.2f}%",
            }

        return jsonify(response)

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/analyze_json', methods=['POST'])
def api_analyze_json():
    """分析股票 API (JSON格式，用于程序调用)"""
    try:
        # 获取参数
        data = request.get_json()
        code = data.get('code', '').strip()
        shares = data.get('shares', 0)
        cost = data.get('cost', 0.0)

        # 验证参数
        if not code:
            return jsonify({'success': False, 'error': '请输入股票代码'})

        # 格式化股票代码
        code = format_stock_code(code)

        # 执行分析
        analyzer_instance = get_analyzer()
        result = analyzer_instance.analyze(code, shares=shares, cost=cost)

        return jsonify({'success': True, 'data': result})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


if __name__ == '__main__':
    print("=" * 50)
    print("股票分析 Web 服务")
    print("=" * 50)
    print("访问地址: http://localhost:5000")
    print("API 端点:")
    print("  - POST /api/analyze (表单提交)")
    print("  - POST /api/analyze_json (JSON提交)")
    print("=" * 50)

    app.run(host='0.0.0.0', port=5000, debug=True)
