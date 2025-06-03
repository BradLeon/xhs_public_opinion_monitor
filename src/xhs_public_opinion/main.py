#!/usr/bin/env python
import sys
import warnings
import os
import json
from datetime import datetime
from dotenv import load_dotenv


from xhs_public_opinion.crew import XhsPublicOpinionCrew
from xhs_public_opinion.tools import (
    DatabaseReaderTool,
    DatabaseWriterTool
)

warnings.filterwarnings("ignore", category=SyntaxWarning, module="pysbd")

# 加载环境变量
load_dotenv()

# This main file is intended to be a way for you to run your
# crew locally, so refrain from adding unnecessary logic into this file.
# Replace with inputs you want to test with, it will automatically
# interpolate any tasks and agents information

def run():
    """
    运行小红书公共舆情分析（重构版本：数据库操作独立执行）
    流程：数据库读取 → AI内容分析 → 数据库写入
    """
    # 检查必要的环境变量
    required_env_vars = ['SEO_SUPABASE_URL', 'SEO_SUPABASE_ANON_KEY', 'OPENROUTER_API_KEY']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"错误：缺少必要的环境变量: {', '.join(missing_vars)}")
        print("请创建 .env 文件并设置以下变量：")
        print("SEO_SUPABASE_URL=your_supabase_url")
        print("SEO_SUPABASE_ANON_KEY=your_supabase_anon_key") 
        print("OPENROUTER_API_KEY=your_openrouter_api_key")
        return None
    
    batch_size = 10  # 每次处理的笔记数量
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        print("🚀 开始执行小红书公共舆情分析（重构版本）...")
        print(f"📅 分析时间: {current_time}")
        print(f"📊 批处理大小: {batch_size} 条笔记")
        print("="*60)
        
        # ==================== 阶段1：数据库读取 ====================
        print("📖 阶段1: 执行数据库读取...")
        db_reader = DatabaseReaderTool()
        raw_data = db_reader._run(limit=batch_size)
        
        # 检查读取结果
        if not raw_data or "没有找到未处理的笔记数据" in raw_data:
            print("⚠️ 没有找到未处理的数据，分析结束")
            return {
                "status": "no_data",
                "message": "没有找到未处理的笔记数据",
                "timestamp": datetime.now().isoformat()
            }
        
        # 解析读取的数据
        try:
            notes_data = json.loads(raw_data)
            notes_count = notes_data.get('total_count', 0)
            notes_list = notes_data.get('notes', [])
            
            if notes_count == 0:
                print("⚠️ 读取到的数据为空，分析结束")
                return {
                    "status": "empty_data",
                    "message": "读取到的数据为空",
                    "timestamp": datetime.now().isoformat()
                }
            
            print(f"✅ 数据读取完成! 共获取 {notes_count} 条笔记数据")
            print(f"📋 笔记ID列表: {[note.get('note_id', 'unknown') for note in notes_list[:3]]}{'...' if len(notes_list) > 3 else ''}")
            print(f"📋 笔记数据: {raw_data}")

        except json.JSONDecodeError as e:
            print(f"❌ 数据解析失败: {e}")
            return None
        
        # ==================== 阶段2：AI内容分析 ====================
        print("\n" + "="*60)
        print("🤖 阶段2: 执行AI内容分析...")
        
        # 准备crew输入（直接提供读取的数据）
        crew_inputs = {
            'analysis_type': '小红书笔记情感分析',
            'current_date': current_time,
            'batch_size': batch_size,
            'notes': notes_list  # 将读取的原始JSON字符串传递给crew
        }
        
        print(f"📋 Crew输入参数: {list(crew_inputs.keys())}")
        print(f"📊 notes长度: {len(notes_list)} 字符")
        
        # 执行AI分析crew（现在只包含内容分析部分）
        crew_result = XhsPublicOpinionCrew().crew().kickoff(inputs=crew_inputs)
        
        if not crew_result:
            print("❌ 错误：AI分析没有返回结果")
            return None
            
        print("✅ AI内容分析完成!")
        analysis_json = str(crew_result)
        print(f"📊 分析结果长度: {len(analysis_json)} 字符", "type of analysis_json:", type(analysis_json))
        print(f"📋 分析结果预览: {analysis_json}")
        
        # ==================== 阶段3：数据库写入 ====================
        print("\n" + "="*60)
        print("💾 阶段3: 执行数据库写入...")
        
        # 使用DatabaseWriterTool进行数据库写入
        db_writer = DatabaseWriterTool()
        write_result = db_writer._run(analysis_json)
        
        print("\n📊 数据库写入结果:")
        print(write_result)
        
        # ==================== 执行总结 ====================
        print("\n" + "="*60)
        print("📋 执行总结:")
        print(f"✅ 数据库读取阶段: 完成 (读取 {notes_count} 条记录)")
        print(f"✅ AI内容分析阶段: 完成")
        print(f"✅ 数据库写入阶段: 完成") 
        print(f"⏰ 总执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
        return {
            "status": "success",
            "data_read_count": notes_count,
            "analysis_result": analysis_json,
            "database_result": write_result,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        print(f"❌ 执行过程中出现错误: {e}")
        print("详细错误信息请查看日志")
        return None

def train():
    """
    训练crew模型
    """
    if len(sys.argv) < 3:
        print("使用方法: python -m xhs_public_opinion.main train <迭代次数> <训练文件名>")
        return None
        
    inputs = {
        "analysis_type": "小红书笔记情感分析训练"
    }
    try:
        print(f"🎯 开始训练模型，迭代次数: {sys.argv[1]}")
        result = XhsPublicOpinionCrew().crew().train(
            n_iterations=int(sys.argv[1]), 
            filename=sys.argv[2], 
            inputs=inputs
        )
        print("✅ 训练完成!")
        return result

    except Exception as e:
        print(f"❌ 训练过程中出现错误: {e}")
        print("详细错误信息请查看日志")
        return None

def replay():
    """
    重放crew执行过程
    """
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
    """
    测试crew功能
    """
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
