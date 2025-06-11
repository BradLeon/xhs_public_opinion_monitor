#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from xhs_public_opinion.tools.sov_visualization_tool import SOVVisualizationTool

def test_sov_visualization():
    """测试SOV可视化工具（数据库模式）"""
    print("🔧 测试SOV可视化工具...")
    
    tool = SOVVisualizationTool()
    
    # 测试1: 普通图表
    print("\n📊 测试1: 生成普通图表...")
    result = tool._run(
        keyword="丰盈蓬松洗发水",
        output_dir="outputs"
    )
    print(f"结果: {result}")
    
    # 测试2: 带目标品牌高亮的图表
    print("\n📈 测试2: 生成带目标品牌高亮的图表...")
    result = tool._run(
        keyword="丰盈蓬松洗发水",
        target_brand="Living Proof",  # 目标品牌高亮
        output_dir="outputs"
    )
    print(f"结果: {result}")
    
    # 测试3: 测试去重统计
    print("\n🧮 测试3: 测试品牌去重统计...")
    result = tool._run(
        keyword="丰盈蓬松洗发水",
        target_brand="Fan Beauty",
        output_dir="outputs"
    )
    print(f"结果: {result}")

if __name__ == "__main__":
    test_sov_visualization() 