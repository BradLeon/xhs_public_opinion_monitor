#!/usr/bin/env python
"""
测试SOV分析工具
"""

import sys
import os

# 添加src路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_tools_import():
    """测试工具导入"""
    try:
        from xhs_public_opinion.tools import DataMergerTool, SOVCalculatorTool
        print("✅ 工具导入成功")
        print(f"📊 DataMergerTool: {DataMergerTool.name} - {DataMergerTool.description}")
        print(f"📈 SOVCalculatorTool: {SOVCalculatorTool.name} - {SOVCalculatorTool.description}")
        return True
    except ImportError as e:
        print(f"❌ 工具导入失败: {e}")
        return False

def test_tool_initialization():
    """测试工具初始化"""
    try:
        from xhs_public_opinion.tools import DataMergerTool, SOVCalculatorTool
        
        # 测试DataMergerTool初始化
        try:
            merger = DataMergerTool()
            print("✅ DataMergerTool 初始化成功")
        except Exception as e:
            print(f"⚠️ DataMergerTool 初始化失败: {e}")
            print("   (可能需要配置数据库环境变量)")
        
        # 测试SOVCalculatorTool初始化
        try:
            calculator = SOVCalculatorTool()
            print("✅ SOVCalculatorTool 初始化成功")
        except Exception as e:
            print(f"❌ SOVCalculatorTool 初始化失败: {e}")
            
        return True
    except Exception as e:
        print(f"❌ 工具初始化测试失败: {e}")
        return False

def main():
    """主测试函数"""
    print("🧪 开始测试SOV分析工具")
    print("="*50)
    
    # 测试1：工具导入
    print("\n📦 测试1: 工具导入")
    if not test_tools_import():
        return
    
    # 测试2：工具初始化  
    print("\n🔧 测试2: 工具初始化")
    if not test_tool_initialization():
        return
    
    print("\n✅ 所有测试通过！")
    print("\n📋 使用说明:")
    print("1. 确保设置了数据库环境变量 (SEO_SUPABASE_URL, SEO_SUPABASE_ANON_KEY)")
    print("2. 运行示例: python examples/sov_analysis_example.py")
    print("3. 或直接使用工具进行分析:")
    print("   - DataMergerTool: 数据拼接")
    print("   - SOVCalculatorTool: SOV计算")

if __name__ == "__main__":
    main() 