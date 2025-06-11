import os
import logging
from datetime import datetime
from typing import List, Dict, Any

from src.xhs_public_opinion.core.config import Settings
from src.xhs_public_opinion.database.supabase_db import SupabaseDatabase
from src.xhs_public_opinion.tools.multimodal_analyzer_tool import MultimodalAnalyzerTool
from src.xhs_public_opinion.tools.data_merger_tool import DataMergerTool
from src.xhs_public_opinion.tools.sov_calculator_tool import SOVCalculatorTool
from src.xhs_public_opinion.tools.brand_sentiment_extractor_tool import BrandSentimentExtractorTool
from src.xhs_public_opinion.tools.specific_notes_reader_tool import SpecificNotesReaderTool
from src.xhs_public_opinion.tools.sov_visualization_tool import SOVVisualizationTool

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """主函数"""
    print("\n🚀 小红书舆情监控系统")
    print("=" * 50)
    
    while True:
        print("\n📋 请选择操作:")
        print("1. 🔍 采集笔记数据")
        print("2. 🤖 多模态分析")
        print("3. 🔄 数据合并排序")
        print("4. 📊 SOV统计分析")
        print("5. 💭 品牌情感分析") 
        print("6. ⚡ 一键运行完整流程")
        print("7. 📈 生成SOV单档位图表")
        print("8. 📋 生成SOV综合报告")
        print("0. 🚪 退出系统")
        
        choice = input("\n请输入选择 (0-8): ").strip()
        
        if choice == "0":
            print("\n👋 谢谢使用，再见!")
            break
        elif choice == "1":
            run_data_collection()
        elif choice == "2":
            run_multimodal_analysis()
        elif choice == "3":
            run_data_merger()
        elif choice == "4":
            run_sov_analysis()
        elif choice == "5":
            run_brand_sentiment()
        elif choice == "6":
            run_complete_pipeline()
        elif choice == "7":
            run_sov_visualization()
        elif choice == "8":
            run_comprehensive_report()
        else:
            print("❌ 无效选择，请重新输入")

def run_data_collection():
    """运行数据采集"""
    print("\n🔍 数据采集功能")
    print("-" * 30)
    print("该功能需要配合爬虫工具使用，请确保已经将数据导入到数据库中")

def run_multimodal_analysis():
    """运行多模态分析"""
    print("\n🤖 多模态分析")
    print("-" * 30)
    
    keywords = input("请输入关键词 (多个关键词用逗号分隔): ").strip()
    if not keywords:
        print("❌ 关键词不能为空")
        return
    
    keyword_list = [k.strip() for k in keywords.split(",")]
    
    for keyword in keyword_list:
        print(f"\n分析关键词: {keyword}")
        try:
            # 1. 数据合并排序，获取前100名
            merger_tool = DataMergerTool()
            merge_result = merger_tool._run(keyword=keyword)
            print(f"数据合并结果: {merge_result}")
            
            # 2. 获取前100名note_id
            top_note_ids = _get_top_note_ids_from_csv(keyword)
            if not top_note_ids:
                print(f"❌ 未找到关键词 {keyword} 的排序数据")
                continue
            
            # 3. 只对前100名进行多模态分析
            _analyze_top_notes(keyword, top_note_ids)
            
            # 4. SOV统计
            sov_tool = SOVCalculatorTool()
            sov_result = sov_tool._run(keyword=keyword)
            print(f"SOV统计结果: {sov_result}")
            
            # 5. 品牌情感分析
            sentiment_tool = BrandSentimentExtractorTool()
            sentiment_result = sentiment_tool._run(keyword=keyword)
            print(f"情感分析结果: {sentiment_result}")
            
        except Exception as e:
            print(f"❌ 关键词 {keyword} 处理失败: {e}")

def run_data_merger():
    """运行数据合并"""
    print("\n🔄 数据合并排序")
    print("-" * 30)
    
    keyword = input("请输入关键词: ").strip()
    if not keyword:
        print("❌ 关键词不能为空")
        return
    
    try:
        tool = DataMergerTool()
        result = tool._run(keyword=keyword)
        print(f"\n{result}")
        
    except Exception as e:
        print(f"❌ 数据合并失败: {e}")

def run_sov_analysis():
    """运行SOV分析"""
    print("\n📊 SOV统计分析")
    print("-" * 30)
    
    keyword = input("请输入关键词: ").strip()
    if not keyword:
        print("❌ 关键词不能为空")
        return
    
    try:
        tool = SOVCalculatorTool()
        result = tool._run(keyword=keyword)
        print(f"\n{result}")
        
    except Exception as e:
        print(f"❌ SOV分析失败: {e}")

def run_brand_sentiment():
    """运行品牌情感分析"""
    print("\n💭 品牌情感分析")
    print("-" * 30)
    
    keyword = input("请输入关键词: ").strip()
    if not keyword:
        print("❌ 关键词不能为空")
        return
    
    try:
        tool = BrandSentimentExtractorTool()
        result = tool._run(keyword=keyword)
        print(f"\n{result}")
        
    except Exception as e:
        print(f"❌ 品牌情感分析失败: {e}")

def run_complete_pipeline():
    """运行完整流程"""
    print("\n⚡ 一键运行完整流程")
    print("-" * 30)
    
    # 提供关键词选择
    print("支持的关键词:")
    print("1. 脱毛")
    print("2. 丰盈蓬松洗发水")
    print("3. 缕灵")
    print("4. livingproof")
    print("5. 自定义关键词")
    print("6. 批量处理所有关键词")
    
    choice = input("\n请选择 (1-6): ").strip()
    keyword_map = {
        "1": ["脱毛"],
        "2": ["丰盈蓬松洗发水"],
        "3": ["缕灵"],
        "4": ["livingproof"],
        "6": ["脱毛", "丰盈蓬松洗发水", "缕灵", "livingproof"]
    }
    
    if choice in keyword_map:
        keyword_list = keyword_map[choice]
    elif choice == "5":
        keywords = input("请输入关键词 (多个关键词用逗号分隔): ").strip()
        if not keywords:
            print("❌ 关键词不能为空")
            return
        keyword_list = [k.strip() for k in keywords.split(",")]
    else:
        print("❌ 无效选择")
        return
    
    for keyword in keyword_list:
        print(f"\n{'='*20} 处理关键词: {keyword} {'='*20}")
        
        try:
            # 1. 数据合并排序
            print("🔄 Step 1: 数据合并排序...")
            merger_tool = DataMergerTool()
            merge_result = merger_tool._run(keyword=keyword)
            print(f"合并结果: {merge_result}")
            
            # 2. 获取前100名note_id进行分析
            print("🔍 Step 2: 获取前100名笔记...")
            top_note_ids = _get_top_note_ids_from_csv(keyword)
            if not top_note_ids:
                print(f"❌ 未找到关键词 {keyword} 的排序数据，跳过")
                continue
            
            # 3. 多模态分析前100名
            print("🤖 Step 3: 多模态分析前100名...")
            _analyze_top_notes(keyword, top_note_ids)
            
            # 4. SOV统计
            print("📊 Step 4: SOV统计分析...")
            sov_tool = SOVCalculatorTool()
            sov_result = sov_tool._run(keyword=keyword)
            print(f"SOV结果: {sov_result}")
            
            # 5. 品牌情感分析
            print("💭 Step 5: 品牌情感分析...")
            sentiment_tool = BrandSentimentExtractorTool()
            sentiment_result = sentiment_tool._run(keyword=keyword)
            print(f"情感结果: {sentiment_result}")
            
            print(f"✅ 关键词 {keyword} 处理完成!")
            
        except Exception as e:
            logger.error(f"关键词 {keyword} 处理失败: {e}")
            print(f"❌ 关键词 {keyword} 处理失败: {e}")

def run_sov_visualization():
    """运行SOV单档位可视化"""
    print("\n📈 SOV单档位图表生成")
    print("-" * 30)
    
    # 提供关键词选择
    print("支持的关键词:")
    print("1. 脱毛仪")
    print("2. 丰盈蓬松洗发水")
    print("3. 缕灵")
    print("4. livingproof")
    print("5. 自定义关键词")
    
    choice = input("\n请选择关键词 (1-5): ").strip()
    keyword_map = {
        "1": "脱毛仪",
        "2": "丰盈蓬松洗发水", 
        "3": "缕灵",
        "4": "livingproof"
    }
    
    if choice in keyword_map:
        keyword = keyword_map[choice]
    elif choice == "5":
        keyword = input("请输入自定义关键词: ").strip()
        if not keyword:
            print("❌ 关键词不能为空")
            return
    else:
        print("❌ 无效选择")
        return
    
    tier = input("请选择档位 (top20/top50/top100，默认 top20): ").strip()
    if not tier:
        tier = "top20"
    
    try:
        tool = SOVVisualizationTool()
        result = tool._run(
            keyword=keyword,
            tier=tier,
            chart_type="single",
            output_dir="outputs"
        )
        print(f"\n{result}")
        
    except Exception as e:
        print(f"❌ 可视化生成失败: {e}")

def run_comprehensive_report():
    """运行综合报告生成"""
    print("\n📋 SOV综合报告生成")
    print("-" * 30)
    
    # 提供关键词选择
    print("支持的关键词:")
    print("1. 脱毛仪")
    print("2. 丰盈蓬松洗发水")
    print("3. 缕灵")
    print("4. livingproof")
    print("5. 自定义关键词")
    
    choice = input("\n请选择关键词 (1-5): ").strip()
    keyword_map = {
        "1": "脱毛仪",
        "2": "丰盈蓬松洗发水",
        "3": "缕灵", 
        "4": "livingproof"
    }
    
    if choice in keyword_map:
        keyword = keyword_map[choice]
    elif choice == "5":
        keyword = input("请输入自定义关键词: ").strip()
        if not keyword:
            print("❌ 关键词不能为空")
            return
    else:
        print("❌ 无效选择")
        return
    
    target_brand = input("请输入目标品牌 (可选): ").strip()
    
    try:
        tool = SOVVisualizationTool()
        result = tool._run(
            keyword=keyword,
            chart_type="comprehensive",
            target_brand=target_brand,
            output_dir="outputs"
        )
        print(f"\n{result}")
        
    except Exception as e:
        print(f"❌ 综合报告生成失败: {e}")

def _get_top_note_ids_from_csv(keyword: str, limit: int = 100) -> List[str]:
    """从CSV文件中获取前N名的note_id"""
    try:
        import pandas as pd
        import glob
        
        # 查找最新的合并数据文件
        pattern = os.path.join("outputs", keyword, "merged_data_*.csv")
        files = glob.glob(pattern)
        
        if not files:
            logger.warning(f"未找到关键词 {keyword} 的合并数据文件")
            return []
        
        # 选择最新文件
        latest_file = max(files, key=os.path.getctime)
        logger.info(f"使用数据文件: {latest_file}")
        
        # 读取数据
        df = pd.read_csv(latest_file, encoding='utf-8-sig')
        
        # 按rank排序，取前N名
        df_sorted = df.sort_values('rank').head(limit)
        note_ids = df_sorted['note_id'].tolist()
        
        logger.info(f"获取到 {len(note_ids)} 个note_id")
        return note_ids
        
    except Exception as e:
        logger.error(f"获取note_id失败: {e}")
        return []

def _analyze_top_notes(keyword: str, note_ids: List[str]):
    """对前100名笔记进行多模态分析"""
    try:
        # 使用SpecificNotesReaderTool读取指定笔记
        reader_tool = SpecificNotesReaderTool()
        notes_result = reader_tool._run(note_ids=",".join(note_ids))
        
        if "✅" not in notes_result:
            logger.warning(f"读取笔记数据失败: {notes_result}")
            return
        
        # 使用MultimodalAnalyzerTool进行分析
        analyzer_tool = MultimodalAnalyzerTool()
        analysis_result = analyzer_tool._run(keyword=keyword)
        
        logger.info(f"多模态分析结果: {analysis_result}")
        
    except Exception as e:
        logger.error(f"分析前100名笔记失败: {e}")

if __name__ == "__main__":
    main() 