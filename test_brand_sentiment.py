#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from xhs_public_opinion.tools.brand_sentiment_visualization_tool import BrandSentimentVisualizationTool

def test_brand_sentiment_visualization():
    """测试品牌情感分析可视化工具"""
    print("🔧 测试品牌情感分析可视化工具...")
    
    tool = BrandSentimentVisualizationTool()
    
    # 测试1: 生成Living Proof品牌的情感分析图表
    print("\n📊 测试1: 生成Living Proof品牌情感分析图表...")
    result = tool._run(
        keyword="丰盈蓬松洗发水",
        target_brand="Living Proof",
        output_dir="outputs"
    )
    print(f"结果: {result}")
    
    # 测试2: 测试其他品牌（如果有数据）
    print("\n📈 测试2: 生成其他品牌情感分析图表...")
    result = tool._run(
        keyword="缕灵",
        target_brand="Living Proof",
        output_dir="outputs"
    )
    print(f"结果: {result}")
    
    # 测试3: 测试无数据情况
    print("\n🧪 测试3: 测试无数据品牌...")
    result = tool._run(
        keyword="丰盈蓬松洗发水", 
        target_brand="Fan Beauty",
        output_dir="outputs"
    )
 
    print(f"结果: {result}")

if __name__ == "__main__":
    test_brand_sentiment_visualization() 