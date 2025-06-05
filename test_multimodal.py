#!/usr/bin/env python
"""
多模态品牌分析器测试脚本
测试文本、图片和视频内容的综合分析能力
"""

import json
import sys
import os
from dotenv import load_dotenv

# 添加项目路径
sys.path.append('src')

from xhs_public_opinion.tools.multimodal_analyzer import MultimodalBrandAnalyzer

# 加载环境变量
load_dotenv()

def test_text_only():
    """测试纯文本分析"""
    print("🧪 测试1: 纯文本分析")
    analyzer = MultimodalBrandAnalyzer()
    
    test_content = {
        "title": "超好用的洗发水推荐！",
        "desc": "最近用了馥绿德雅的丰盈蓬松洗发水，真的太棒了！头发变得很蓬松，而且味道也很好闻。相比之下，之前用的卡诗就一般般了。推荐大家试试！",
        "image_list": [],
        "video_url": "",
        "type": "normal"
    }
    
    result = analyzer._run(json.dumps(test_content, ensure_ascii=False))
    print_result("纯文本分析", result)

def test_text_with_images():
    """测试文本+图片分析"""
    print("\n🧪 测试2: 文本+图片分析")
    analyzer = MultimodalBrandAnalyzer()
    
    test_content = {
        "title": "我的护肤品分享",
        "desc": "今天分享一下最近在用的护肤品，兰蔻的小黑瓶精华真的很不错，雅诗兰黛的面霜也很滋润。",
        "image_list": [
            "https://example.com/lancome_product.jpg",
            "https://example.com/estee_lauder_cream.jpg"
        ],
        "video_url": "",
        "type": "normal"
    }
    
    result = analyzer._run(json.dumps(test_content, ensure_ascii=False))
    print_result("文本+图片分析", result)

def test_video_content():
    """测试视频内容分析"""
    print("\n🧪 测试3: 视频内容分析")
    analyzer = MultimodalBrandAnalyzer()
    
    test_content = {
        "title": "化妆品试用视频",
        "desc": "今天试用了几款口红，给大家做个对比",
        "image_list": [],
        "video_url": "https://example.com/makeup_review.mp4",
        "type": "video"
    }
    
    result = analyzer._run(json.dumps(test_content, ensure_ascii=False))
    print_result("视频内容分析", result)

def test_complex_multimodal():
    """测试复杂多模态内容"""
    print("\n🧪 测试4: 复杂多模态分析")
    analyzer = MultimodalBrandAnalyzer()
    
    test_content = {
        "title": "全套护肤流程分享",
        "desc": "从洁面到保湿，完整的护肤流程。包括科颜氏的洁面乳、SK-II的神仙水、雅诗兰黛的眼霜等。效果真的很棒！",
        "image_list": [
            "https://example.com/skincare_routine1.jpg",
            "https://example.com/skincare_routine2.jpg",
            "https://example.com/skincare_products.jpg"
        ],
        "video_url": "https://example.com/skincare_demo.mp4",
        "type": "video"
    }
    
    result = analyzer._run(json.dumps(test_content, ensure_ascii=False))
    print_result("复杂多模态分析", result)

def print_result(test_name: str, result: str):
    """打印测试结果"""
    try:
        parsed_result = json.loads(result)
        print(f"📊 {test_name}结果:")
        print(f"   品牌列表: {parsed_result.get('brand_list', [])}")
        print(f"   SPU列表: {parsed_result.get('spu_list', [])}")
        print(f"   情感分析: {parsed_result.get('emotion_dict', {})}")
        print(f"   评价词汇: {parsed_result.get('evaluation_dict', {})}")
        
        if parsed_result.get('_analysis_failed', False):
            print(f"   ⚠️ 分析失败: {parsed_result.get('_error_message', 'Unknown error')}")
    except json.JSONDecodeError as e:
        print(f"❌ 结果解析失败: {e}")
        print(f"原始结果: {result[:200]}...")

def test_client_initialization():
    """测试客户端初始化"""
    print("🔧 测试0: 客户端初始化")
    analyzer = MultimodalBrandAnalyzer()
    
    if analyzer.client:
        print("✅ MultimodalBrandAnalyzer客户端初始化成功")
        return True
    else:
        print("❌ MultimodalBrandAnalyzer客户端初始化失败")
        print("请检查OPENROUTER_API_KEY环境变量是否设置")
        return False

def main():
    """主测试函数"""
    print("🚀 多模态品牌分析器测试开始")
    print("="*60)
    
    # 检查客户端初始化
    if not test_client_initialization():
        return
    
    print("="*60)
    
    try:
        # 执行各项测试
        test_text_only()
        test_text_with_images()
        test_video_content()
        test_complex_multimodal()
        
        print("\n" + "="*60)
        print("✅ 所有测试完成！")
        print("📝 注意: 这是模拟测试，实际效果需要配置正确的API密钥")
        
    except Exception as e:
        print(f"\n❌ 测试过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 