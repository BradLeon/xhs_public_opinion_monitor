#!/usr/bin/env python
"""
测试简化后的AdvancedBrandAnalyzer
"""

import sys
import os
import json

# 添加src路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_simplified_analyzer():
    """测试简化后的分析器"""
    from xhs_public_opinion.tools.advanced_analyzer import AdvancedBrandAnalyzer
    
    # 测试用例
    test_content = """
    {
        "notes": [
            {
                "note_id": "test001",
                "title": "兰蔻粉底液使用心得",
                "desc": "这款兰蔻粉底液真的太好用了！遮瑕力强，持妆时间长，特别推荐给油皮姐妹。质地轻薄不厚重，自然提亮肤色。",
                "type": "normal"
            }
        ]
    }
    """
    
    print("🧪 测试简化后的AdvancedBrandAnalyzer")
    print("="*50)
    
    try:
        # 创建分析器实例
        analyzer = AdvancedBrandAnalyzer()
        
        # 执行分析
        print("📋 开始分析...")
        result = analyzer._run(test_content)
        
        print(f"✅ 分析完成!")
        print(f"📄 原始结果: {result}")
        
        # 尝试解析JSON
        try:
            parsed_result = json.loads(result)
            print(f"📊 解析后结果:")
            for key, value in parsed_result.items():
                if not key.startswith('_'):
                    print(f"   {key}: {value}")
                else:
                    print(f"   {key}: {value} (错误标记)")
            
            # 检查是否有错误标记
            if parsed_result.get('_analysis_failed', False):
                print(f"❌ 分析失败: {parsed_result.get('_error_message', 'unknown')}")
                print(f"🔍 错误类型: {parsed_result.get('_error_type', 'unknown')}")
            else:
                print("✅ 分析成功，无错误标记")
                
        except json.JSONDecodeError as e:
            print(f"❌ JSON解析失败: {e}")
            print(f"原始结果: {result}")
            
    except Exception as e:
        print(f"❌ 测试过程出错: {e}")
        import traceback
        traceback.print_exc()

def test_error_handling():
    """测试错误处理机制"""
    from xhs_public_opinion.tools.advanced_analyzer import AdvancedBrandAnalyzer
    
    print("\n🧪 测试错误处理机制")
    print("="*50)
    
    # 测试无效输入
    test_cases = [
        ("空字符串", ""),
        ("无效JSON", "这不是JSON"),
        ("空内容", "{}"),
    ]
    
    analyzer = AdvancedBrandAnalyzer()
    
    for name, content in test_cases:
        print(f"\n📋 测试用例: {name}")
        try:
            result = analyzer._run(content)
            parsed = json.loads(result)
            
            if parsed.get('_analysis_failed', False):
                print(f"✅ 正确处理错误: {parsed.get('_error_type')}")
                print(f"   错误信息: {parsed.get('_error_message', 'no message')[:50]}...")
            else:
                print(f"✅ 正常分析结果: {parsed}")
                
        except Exception as e:
            print(f"❌ 测试异常: {e}")

if __name__ == "__main__":
    test_simplified_analyzer()
    test_error_handling() 