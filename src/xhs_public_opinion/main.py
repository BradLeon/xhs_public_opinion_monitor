#!/usr/bin/env python
import sys
import warnings
import os
import json
import re
import pandas as pd
from typing import List
from datetime import datetime
from dotenv import load_dotenv
import logging

from xhs_public_opinion.crew import XhsPublicOpinionCrew
from xhs_public_opinion.tools import (
    DataMergerTool,
    SOVCalculatorTool,
    MultimodalBrandAnalyzer,
    BrandSentimentExtractorTool,
    SOVVisualizationTool,
    BrandSentimentVisualizationTool
)
from xhs_public_opinion.store.database import SupabaseDatabase

logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore", category=SyntaxWarning, module="pysbd")

# 加载环境变量
load_dotenv()

def _check_environment() -> bool:
    """检查必要的环境变量"""
    required_env_vars = ['SEO_SUPABASE_URL', 'SEO_SUPABASE_ANON_KEY', 'DASHSCOPE_API_KEY']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"错误：缺少必要的环境变量: {', '.join(missing_vars)}")
        print("请创建 .env 文件并设置以下变量：")
        for var in missing_vars:
            print(f"{var}=your_{var.lower()}")
        return False
    return True

def run():
    """
    运行小红书公共舆情分析（优化版本：先排序，再对前100名进行多模态分析）
    流程：数据合并排序 → 前100名多模态分析 → SOV统计 → 品牌情感分析 → 可视化图表生成
    """
    try:
        # 环境检查
        if not _check_environment():
            return None
        
        print("🚀 开始执行小红书公共舆情分析（优化版本）...")
        print(f"📅 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
        # 处理关键词列表
        keywords = ["丰盈蓬松洗发水", "缕灵", "livingproof"]
        target_brand = "Living Proof"

        for keyword in keywords:
            print(f"\n🔍 处理关键词: {keyword}")
            print("-" * 40)
            
            
            # 步骤1: 数据合并和排序
            print("📊 步骤1: 数据合并和排序...")
            merged_data_path = _basic_data_merger(keyword=keyword)
            if not merged_data_path or "失败" in merged_data_path:
                print(f"   ❌ 数据合并失败: {merged_data_path}")
                continue
            
            # 步骤2: 提取前100名note_id并进行多模态分析
            
            print("🤖 步骤2: 前100名多模态分析...")
            analysis_success = _analyze_top_notes(csv_path=merged_data_path, top_n=100)
            if not analysis_success:
                print("   ⚠️ 多模态分析失败，但继续后续步骤")
            
            # 步骤3: SOV计算
            print("📈 步骤3: SOV计算...")
            _sov_calculator(keyword=keyword)
            
            # 步骤4: 品牌情感分析
            print("💝 步骤4: 品牌情感分析...")
            _extract_brand_sentiment(keyword=keyword, brand=target_brand, csv_input_path=merged_data_path)
            
            # 步骤5: SOV可视化图表生成
            print("📊 步骤5: SOV可视化图表生成...")
            _sov_visualization(keyword=keyword, target_brand=target_brand)

            # 步骤6: 品牌情感分析可视化图表生成
            print("💝 步骤6: 品牌情感分析可视化图表生成...")
            _brand_sentiment_visualization(keyword=keyword, target_brand=target_brand)

            print(f"✅ 关键词 '{keyword}' 处理完成")
            
        
        print("\n" + "="*60)
        print("🎉 所有关键词处理完成!")
        print(f"⏰ 完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
        return True
        
    except Exception as e:
        print(f"❌ 执行过程中出现错误: {e}")
        print("详细错误信息请查看日志")
        logger.error(f"执行异常: {str(e)}", exc_info=True)
        return None

def _basic_data_merger(keyword: str) -> str:
    """构造基础数据集"""
    print(f"   🔄 合并关键词 '{keyword}' 的搜索结果和笔记详情...")
    data_merger_tool = DataMergerTool()
    res = data_merger_tool._run(keyword)
    return res

def _get_top_note_ids_from_csv(csv_path: str, top_n: int = 100) -> List[str]:
    """从CSV文件中提取前N名的note_id"""
    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        
        # 按rank排序，取前N名
        df_sorted = df.sort_values('rank').head(top_n)
        note_ids = df_sorted['note_id'].tolist()
        
        logger.info(f"从CSV文件提取前{top_n}名note_id: {len(note_ids)}个")
        return note_ids
        
    except Exception as e:
        logger.error(f"从CSV提取note_id失败: {e}")
        return []

def _analyze_top_notes(csv_path: str, top_n: int = 100) -> bool:
    """对前N名笔记进行多模态分析"""
    try:
        # 初始化数据库连接
        db = SupabaseDatabase()
        if not db.is_connected():
            print("   ❌ 数据库连接失败")
            return False
        
        # 1. 从CSV中提取前N名的note_id
        top_note_ids = _get_top_note_ids_from_csv(csv_path, top_n)
        if not top_note_ids:
            print("   ❌ 无法提取前100名note_id")
            return False
        
        print(f"   📋 提取到前{top_n}名笔记，共{len(top_note_ids)}个")
        
        # 2. 直接从数据库读取这些笔记的详细数据
        raw_data = db.get_specific_notes_json(top_note_ids)
        
        if not raw_data or "没有找到" in raw_data:
            print("   ❌ 无法从数据库读取笔记详情")
            return False
        
        # 3. 解析数据
        notes_data = json.loads(raw_data)
        all_notes = notes_data.get('notes', [])
        total_notes = len(all_notes)
        
        print(f"   📖 从数据库读取到{total_notes}条笔记详情")
        
        if total_notes == 0:
            print("   ⚠️ 没有需要分析的笔记")
            return True
        
        # 4. 初始化多模态分析器
        multimodal_analyzer = MultimodalBrandAnalyzer()
        if not multimodal_analyzer.client:
            print("   ❌ 多模态分析器初始化失败，请检查DASHSCOPE_API_KEY")
            return False
        
        # 5. 处理每条笔记
        success_count = 0
        failed_count = 0
        
        for i, note in enumerate(all_notes, 1):
            try:
                print(f"   🔄 分析第{i}/{total_notes}条笔记 (note_id: {note.get('note_id', 'unknown')})...")
                
                # 确定内容类型
                content_type = "video" if note.get('type') == 'video' and note.get('video_url') else \
                             "image" if note.get('image_list') else "text"
                
                # 分析内容
                result = multimodal_analyzer._run(json.dumps(note, ensure_ascii=False), content_type)
                parsed_result = json.loads(result)
                
                if parsed_result.get('_analysis_failed', False):
                    print(f"      ⚠️ 分析失败: {parsed_result.get('_error_message', '未知错误')}")
                    failed_count += 1
                    continue
                
                # 添加笔记ID并直接写入数据库
                parsed_result['note_id'] = note['note_id']
                write_result = db.update_single_note_analysis_json(parsed_result)
                
                # 解析写入结果
                try:
                    write_response = json.loads(write_result)
                    if write_response.get('success', False):
                        success_count += 1
                        print(f"      ✅ 分析完成")
                    else:
                        failed_count += 1
                        print(f"      ❌ 写入失败: {write_response.get('message', '未知错误')}")
                except:
                    # 如果解析失败，根据返回内容判断
                    if "✅" in write_result:
                        success_count += 1
                        print(f"      ✅ 分析完成")
                    else:
                        failed_count += 1
                        print(f"      ❌ 写入失败: {write_result}")
                    
            except Exception as e:
                failed_count += 1
                print(f"      ❌ 处理异常: {e}")
                logger.error(f"处理笔记异常: {str(e)}", exc_info=True)
        
        # 6. 打印统计结果
        print(f"   📊 多模态分析完成:")
        print(f"      ✅ 成功: {success_count} 条")
        print(f"      ❌ 失败: {failed_count} 条")
        print(f"      📈 成功率: {(success_count/total_notes*100):.1f}%")
        
        return success_count > 0
        
    except Exception as e:
        print(f"   ❌ 多模态分析异常: {e}")
        logger.error(f"多模态分析异常: {str(e)}", exc_info=True)
        return False

def _sov_calculator(keyword: str) -> bool:
    """计算SOV"""
    print(f"   📊 计算关键词 '{keyword}' 的SOV...")
    sov_calculator_tool = SOVCalculatorTool()
    res = sov_calculator_tool._run(keyword, method="simple")
    
    if "计算失败" not in res:
        print("   ✅ SOV计算完成")
        return True
    else:
        print(f"   ❌ SOV计算失败: {res}")
        return False

def _extract_brand_sentiment(keyword: str, brand: str = "", csv_input_path: str = "") -> bool:
    """提取品牌情感分析结果"""
    brand_name = brand or "所有品牌"
    print(f"   💝 提取品牌情感分析 - {brand_name}...")
    
    brand_sentiment_extractor = BrandSentimentExtractorTool()
    res = brand_sentiment_extractor._run(keyword=keyword, brand=brand, csv_input_path=csv_input_path)
    
    if "处理失败" not in res:
        print("   ✅ 品牌情感分析完成")
        return True
    else:
        print(f"   ❌ 品牌情感分析失败: {res}")
        return False

def _sov_visualization(keyword: str, target_brand: str = "") -> bool:
    """生成SOV可视化图表"""
    target_info = f" (目标品牌: {target_brand})" if target_brand else ""
    print(f"   📊 生成SOV可视化图表{target_info}...")
    
    sov_visualization_tool = SOVVisualizationTool()
    res = sov_visualization_tool._run(keyword=keyword, target_brand=target_brand)
    
    if "✅" in res:
        print("   ✅ SOV图表生成完成")
        return True
    else:
        print(f"   ❌ SOV图表生成失败: {res}")
        return False

def _brand_sentiment_visualization(keyword: str, target_brand: str) -> bool:
    """生成品牌情感分析可视化图表"""
    print(f"   💝 生成品牌情感分析图表 - {target_brand}...")
    
    brand_sentiment_visualization_tool = BrandSentimentVisualizationTool()
    res = brand_sentiment_visualization_tool._run(keyword=keyword, target_brand=target_brand)
    
    if "✅" in res:
        print("   ✅ 品牌情感分析图表生成完成")
        return True
    else:
        print(f"   ❌ 品牌情感分析图表生成失败: {res}")
        return False

def train():
    """训练crew模型"""
    if len(sys.argv) < 3:
        print("使用方法: python -m xhs_public_opinion.main train <迭代次数> <训练文件名>")
        return None
        
    try:
        print(f"🎯 开始训练模型，迭代次数: {sys.argv[1]}")
        result = XhsPublicOpinionCrew().crew().train(
            n_iterations=int(sys.argv[1]), 
            filename=sys.argv[2], 
            inputs={"analysis_type": "小红书笔记情感分析训练"}
        )
        print("✅ 训练完成!")
        return result

    except Exception as e:
        print(f"❌ 训练过程中出现错误: {e}")
        print("详细错误信息请查看日志")
        return None

def replay():
    """重放crew执行过程"""
    if len(sys.argv) < 2:
        print("使用方法: python -m xhs_public_opinion.main replay <任务ID>")
        return None
        
    try:
        print(f"🔄 开始重放任务: {sys.argv[1]}")
        result = XhsPublicOpinionCrew().crew().replay(task_id=sys.argv[1])
        print("✅ 重放完成!")
        return result

    except Exception as e:
        print(f"❌ 重放过程中出现错误: {e}")
        print("详细错误信息请查看日志")
        return None

def test():
    """测试crew功能"""
    try:
        print("🧪 开始测试crew功能...")
        result = XhsPublicOpinionCrew().crew().test(
            n_iterations=1,
            openai_model_name="gpt-4o-mini"
        )
        print("✅ 测试完成!")
        return result

    except Exception as e:
        print(f"❌ 测试过程中出现错误: {e}")
        print("详细错误信息请查看日志")
        return None

if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == "train":
            train()
        elif command == "replay": 
            replay()
        elif command == "test":
            test()
        else:
            print("未知命令，使用 run() 执行默认分析")
            run()
    else:
        run()
