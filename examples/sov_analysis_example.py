#!/usr/bin/env python
"""
SOV分析示例
演示如何使用DataMergerTool和SOVCalculatorTool进行品牌声量分析
"""

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from dotenv import load_dotenv
from xhs_public_opinion.tools import DataMergerTool, SOVCalculatorTool

# 加载环境变量
load_dotenv()

def main():
    """SOV分析主函数"""
    print("🚀 开始SOV分析示例")
    print("="*60)
    
    # 设置分析参数
    keyword = "面膜"  # 可以根据需要修改关键词
    output_dir = "data/export"
    
    # 步骤1：数据拼接
    print("\n📊 步骤1: 数据拼接")
    print("-"*40)
    
    try:
        merger = DataMergerTool()
        merge_result = merger._run(keyword=keyword, output_dir=output_dir)
        print(merge_result)
        
        if "数据拼接完成" not in merge_result:
            print("❌ 数据拼接失败，请检查数据库中是否有对应关键词的数据")
            return
            
    except Exception as e:
        print(f"❌ 数据拼接出错: {e}")
        return
    
    # 步骤2：SOV计算 - 多种方法
    print("\n📈 步骤2: SOV计算")
    print("-"*40)
    
    calculator = SOVCalculatorTool()
    
    # 方法1：简单计数法
    print("\n🔢 方法1: 简单计数法")
    try:
        simple_result = calculator._run(
            keyword=keyword, 
            data_dir=output_dir, 
            method="simple"
        )
        print(simple_result)
    except Exception as e:
        print(f"❌ 简单计数法计算失败: {e}")
    
    # 方法2：排名加权法
    print("\n⚖️ 方法2: 排名加权法")
    try:
        weighted_result = calculator._run(
            keyword=keyword, 
            data_dir=output_dir, 
            method="weighted"
        )
        print(weighted_result)
    except Exception as e:
        print(f"❌ 排名加权法计算失败: {e}")
    
    # 方法3：互动量加权法
    print("\n💫 方法3: 互动量加权法")
    try:
        engagement_result = calculator._run(
            keyword=keyword, 
            data_dir=output_dir, 
            method="engagement"
        )
        print(engagement_result)
    except Exception as e:
        print(f"❌ 互动量加权法计算失败: {e}")
    
    print("\n✅ SOV分析完成！")
    print("="*60)
    print(f"📁 结果文件保存在: {output_dir}")
    print("📊 您可以查看生成的CSV和JSON文件了解详细结果")

def analyze_keyword(keyword: str):
    """分析指定关键词的SOV"""
    print(f"🎯 开始分析关键词: {keyword}")
    
    # 数据拼接
    merger = DataMergerTool()
    merge_result = merger._run(keyword=keyword)
    print("📊 数据拼接结果:")
    print(merge_result)
    
    # SOV计算
    calculator = SOVCalculatorTool()
    sov_result = calculator._run(keyword=keyword, method="weighted")
    print("\n📈 SOV计算结果:")
    print(sov_result)

if __name__ == "__main__":
    # 检查命令行参数
    if len(sys.argv) > 1:
        keyword = sys.argv[1]
        analyze_keyword(keyword)
    else:
        main() 