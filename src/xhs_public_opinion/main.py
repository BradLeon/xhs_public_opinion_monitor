#!/usr/bin/env python
import sys
import warnings
import os
import json
import re
from datetime import datetime
from dotenv import load_dotenv
import logging

from xhs_public_opinion.crew import XhsPublicOpinionCrew
from xhs_public_opinion.tools import (
    DatabaseReaderTool,
    DataMergerTool,
    SOVCalculatorTool,
    MultimodalBrandAnalyzer,
    SingleNoteWriterTool,
    BrandSentimentExtractorTool
)

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
    运行小红书公共舆情分析（多模态版本：支持文本、图片和视频内容）
    流程：数据库读取 → AI内容分析 → 数据库写入
    """
    try:
        '''
        # 环境检查
        if not _check_environment():
            return None
        
        print("🚀 开始执行小红书公共舆情分析（多模态版本）...")
        print(f"📅 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
        # 初始化工具
        multimodal_analyzer = MultimodalBrandAnalyzer()
        if not multimodal_analyzer.client:
            print("❌ 多模态分析器初始化失败，请检查DASHSCOPE_API_KEY")
            return None
        
        db_reader = DatabaseReaderTool()
        db_writer = SingleNoteWriterTool()
        
        # 读取待处理数据
        raw_data = db_reader._run(batch_size=100)
        if not raw_data or "没有找到未处理的笔记数据" in raw_data:
            print("⚠️ 没有找到未处理的数据")
            return None
        
        # 解析数据
        notes_data = json.loads(raw_data)
        all_notes = notes_data.get('notes', [])
        total_notes = len(all_notes)
        
        print(f"📋 共读取 {total_notes} 条待处理笔记")
        print("="*60)
        
        # 处理每条笔记
        success_count = 0
        failed_count = 0
        
        for i, note in enumerate(all_notes, 1):
            try:
                print(f"\n🔄 处理第 {i}/{total_notes} 条笔记...")
                
                # 确定内容类型
                content_type = "video" if note.get('type') == 'video' and note.get('video_url') else \
                             "image" if note.get('image_list') else "text"
                
                # 分析内容
                result = multimodal_analyzer._run(json.dumps(note, ensure_ascii=False), content_type)
                parsed_result = json.loads(result)
                
                if parsed_result.get('_analysis_failed', False):
                    print(f"   ⚠️ 分析失败: {parsed_result.get('_error_message', '未知错误')}")
                    failed_count += 1
                    continue
                
                # 添加笔记ID并写入数据库
                parsed_result['note_id'] = note['note_id']
                write_result = db_writer._run(parsed_result)
                
                if "✅" in write_result:
                    success_count += 1
                    print(f"   ✅ 处理完成")
                else:
                    failed_count += 1
                    print(f"   ❌ 写入失败: {write_result}")
                    
            except Exception as e:
                failed_count += 1
                print(f"   ❌ 处理异常: {e}")
                logger.error(f"处理异常: {str(e)}", exc_info=True)
        
        # 打印统计结果
        print("\n" + "="*60)
        print("📊 处理完成统计:")
        print(f"✅ 成功: {success_count} 条")
        print(f"❌ 失败: {failed_count} 条")
        print(f"📈 成功率: {(success_count/total_notes*100):.1f}%")
        print(f"⏰ 完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        '''
        success_count = 1

        # 后续处理
        if success_count > 0:
            merged_data_path = _basic_data_merger(keyword="丰盈蓬松洗发水")
            _sov_calculator(keyword="丰盈蓬松洗发水")
            _extract_brand_sentiment(keyword="丰盈蓬松洗发水", brand="Living Proof", csv_input_path=merged_data_path)  # 可以指定特定品牌或留空提取所有品牌
        
        return True
        
    except Exception as e:
        print(f"❌ 执行过程中出现错误: {e}")
        print("详细错误信息请查看日志")
        return None

def _basic_data_merger(keyword: str) -> str:
    """构造基础数据集"""
    data_merger_tool = DataMergerTool()
    res = data_merger_tool._run(keyword)
    return res


def _sov_calculator(keyword: str) -> bool:
    """计算SOV"""
    print("📖 计算SOV...")
    sov_calculator_tool = SOVCalculatorTool()
    res = sov_calculator_tool._run(keyword, method="simple")
    return "计算失败" not in res

def _extract_brand_sentiment(keyword: str, brand: str = "", csv_input_path: str = "") -> bool:
    """提取品牌情感分析结果"""
    print(f"💝 提取品牌情感分析 - keyword: {keyword}, brand: {brand or '所有品牌'}...")
    brand_sentiment_extractor = BrandSentimentExtractorTool()
    res = brand_sentiment_extractor._run(keyword=keyword, brand=brand, csv_input_path=csv_input_path)
    
    if "处理失败" in res:
        print(f"   ❌ 品牌情感分析失败: {res}")
        return False
    else:
        return True

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
