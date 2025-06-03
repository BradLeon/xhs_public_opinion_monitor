#!/usr/bin/env python
import sys
import warnings
import os
import json
import re
from datetime import datetime
from dotenv import load_dotenv


from xhs_public_opinion.crew import XhsPublicOpinionCrew
from xhs_public_opinion.tools import (
    DatabaseReaderTool,
    DatabaseWriterTool
)
from xhs_public_opinion.config.batch_config import BatchConfig

warnings.filterwarnings("ignore", category=SyntaxWarning, module="pysbd")

# 加载环境变量
load_dotenv()

# This main file is intended to be a way for you to run your
# crew locally, so refrain from adding unnecessary logic into this file.
# Replace with inputs you want to test with, it will automatically
# interpolate any tasks and agents information

def run():
    """
    运行小红书公共舆情分析（重构版本：数据库批量操作，AI分批处理）
    流程：大批量数据库读取 → 分批AI内容分析 → 批量数据库写入
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
    
    # 使用配置类获取批次大小
    db_batch_size, ai_batch_size = BatchConfig.validate_and_adjust()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        print("🚀 开始执行小红书公共舆情分析（批量数据库+分批AI处理）...")
        print(f"📅 分析时间: {current_time}")
        print(BatchConfig.get_config_summary())
        print("="*60)
        
        # ==================== 阶段1：批量数据库读取 ====================
        print("📖 阶段1: 批量数据库读取...")
        db_reader = DatabaseReaderTool()
        raw_data = db_reader._run(batch_size=db_batch_size)
        
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
            total_notes_count = notes_data.get('total_count', 0)
            all_notes_list = notes_data.get('notes', [])
            
            if total_notes_count == 0:
                print("⚠️ 读取到的数据为空，分析结束")
                return {
                    "status": "empty_data",
                    "message": "读取到的数据为空",
                    "timestamp": datetime.now().isoformat()
                }
            
            print(f"✅ 数据读取完成! 共获取 {total_notes_count} 条笔记数据")
            print(f"📋 将分批进行AI分析，每批 {ai_batch_size} 条")
            
        except json.JSONDecodeError as e:
            print(f"❌ 数据解析失败: {e}")
            return None
        
        # ==================== 阶段2：分批AI内容分析 + 即时写入 ====================
        print("\n" + "="*60)
        print("🤖 阶段2: 分批执行AI内容分析 + 即时数据库写入...")
        
        total_analysis_results = 0  # 记录总的分析结果数量
        total_written_results = 0   # 记录总的写入成功数量
        failed_batches = []         # 记录失败的批次
        successful_batches = []     # 记录成功的批次
        
        # 初始化数据库写入工具（复用同一个实例）
        db_writer = DatabaseWriterTool()
        
        # 将数据分批处理
        for batch_index in range(0, total_notes_count, ai_batch_size):
            batch_end = min(batch_index + ai_batch_size, total_notes_count)
            current_batch = all_notes_list[batch_index:batch_end]
            batch_number = (batch_index // ai_batch_size) + 1
            total_batches = (total_notes_count + ai_batch_size - 1) // ai_batch_size
            
            print(f"\n🔄 处理第 {batch_number}/{total_batches} 批 (索引 {batch_index}~{batch_end-1}, 共 {len(current_batch)} 条)")
            
            try:
                # ===== AI分析阶段 =====
                # 准备crew输入
                crew_inputs = {
                    'analysis_type': f'小红书笔记情感分析 - 第{batch_number}批',
                    'current_date': current_time,
                    'batch_size': len(current_batch),
                    'notes': current_batch
                }
                
                print(f"📋 开始AI分析: {len(current_batch)} 条笔记")
                print(f"📋 开始AI分析 crew_inputs: {crew_inputs}")
                
                # 执行AI分析crew
                crew_result = XhsPublicOpinionCrew().crew().kickoff(inputs=crew_inputs)
                
                if not crew_result:
                    print(f"❌ 第{batch_number}批AI分析失败：没有返回结果")
                    failed_batches.append({
                        'batch': batch_number,
                        'error': 'AI分析无返回结果',
                        'note_count': len(current_batch)
                    })
                    continue
                    
                print(f"✅ 第{batch_number}批AI分析完成!")
                analysis_json = str(crew_result)
                print(f"📊 分析结果长度: {len(analysis_json)} 字符")
                
                # 解析分析结果
                try:
                    if isinstance(crew_result, str):
                        batch_results = json.loads(analysis_json)
                    else:
                        batch_results = json.loads(str(crew_result))
                    
                    if not isinstance(batch_results, list):
                        print(f"⚠️ 第{batch_number}批结果格式不正确（非数组），跳过")
                        failed_batches.append({
                            'batch': batch_number,
                            'error': '结果格式不正确',
                            'note_count': len(current_batch)
                        })
                        continue
                    
                    total_analysis_results += len(batch_results)
                    print(f"📊 第{batch_number}批解析成功，获得 {len(batch_results)} 条分析结果")
                    
                except json.JSONDecodeError as e:
                    print(f"⚠️ 第{batch_number}批结果解析失败: {e}")
                    print(f"原始结果预览: {analysis_json[:200]}...")
                    failed_batches.append({
                        'batch': batch_number,
                        'error': f'JSON解析失败: {e}',
                        'note_count': len(current_batch)
                    })
                    continue
                
                # ===== 立即数据库写入阶段 =====
                print(f"💾 立即写入第{batch_number}批分析结果到数据库...")
                
                # 将当前批次结果转换为JSON字符串
                batch_json = json.dumps(batch_results, ensure_ascii=False)
                print(f"📋 立即写入数据库 batch_json: {batch_json}")


                
                # 立即写入数据库
                write_result = db_writer._run(batch_json)
                
                # 解析写入结果
                if "成功写入" in write_result or "✅" in write_result:
                    # 从写入结果中提取成功数量（简单的字符串匹配）
                    success_match = re.search(r'成功写入[：:]\s*(\d+)', write_result)
                    if success_match:
                        batch_written_count = int(success_match.group(1))
                    else:
                        batch_written_count = len(batch_results)  # 假设全部成功
                    
                    total_written_results += batch_written_count
                    successful_batches.append({
                        'batch': batch_number,
                        'analyzed': len(batch_results),
                        'written': batch_written_count
                    })
                    print(f"✅ 第{batch_number}批数据库写入完成: {batch_written_count} 条")
                else:
                    batch_written_count = 0  # 写入失败时设置为0
                    print(f"❌ 第{batch_number}批数据库写入失败")
                    failed_batches.append({
                        'batch': batch_number,
                        'error': '数据库写入失败',
                        'note_count': len(current_batch)
                    })
                
                print(f"📊 第{batch_number}批处理结果: AI分析 {len(batch_results)} 条 → 数据库写入 {batch_written_count} 条")
                    
            except Exception as e:
                print(f"❌ 第{batch_number}批处理出错: {e}")
                failed_batches.append({
                    'batch': batch_number,
                    'error': str(e),
                    'note_count': len(current_batch)
                })
                continue
        
        # ==================== 处理结果统计 ====================
        print(f"\n📊 分批处理完成统计:")
        print(f"✅ 成功批次: {len(successful_batches)}")
        print(f"❌ 失败批次: {len(failed_batches)}")
        print(f"📈 总AI分析结果: {total_analysis_results} 条")
        print(f"💾 总数据库写入: {total_written_results} 条")
        
        # ==================== 执行总结 ====================
        print("\n" + "="*60)
        print("📋 执行总结:")
        print(f"✅ 数据库读取阶段: 完成 (读取 {total_notes_count} 条记录)")
        print(f"✅ AI内容分析阶段: 完成 (分 {(total_notes_count + ai_batch_size - 1) // ai_batch_size} 批处理)")
        print(f"✅ 数据库写入阶段: 完成 (写入 {total_written_results} 条分析结果)") 
        print(f"⏰ 总执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
        return {
            "status": "success",
            "data_read_count": total_notes_count,
            "analysis_batches": (total_notes_count + ai_batch_size - 1) // ai_batch_size,
            "analysis_results_count": total_analysis_results,
            "final_results": {
                "successful_batches": successful_batches,
                "failed_batches": failed_batches
            },
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
