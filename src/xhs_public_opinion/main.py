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
    DatabaseWriterTool,
    DataMergerTool,
    SOVCalculatorTool
)
from xhs_public_opinion.config.batch_config import BatchConfig

logger = logging.getLogger(__name__)

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
    # 环境检查
    if not _check_environment():
        return None
    
    # 初始化配置
    db_batch_size, ai_batch_size = BatchConfig.validate_and_adjust()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        _print_startup_info(current_time)
        
        # 阶段1：批量数据库读取
        '''
        notes_data = _read_database_batch(db_batch_size)
        if not notes_data:
            return _create_empty_result("no_data", "没有找到未处理的笔记数据")
        
        total_notes_count = notes_data['total_count']
        all_notes_list = notes_data['notes']
        
        # 阶段2：分批AI分析和写入
        stats = _process_all_batches(all_notes_list, ai_batch_size, current_time)
        
        # 结果统计和总结
        _print_final_statistics(stats, total_notes_count, ai_batch_size)
        '''

        # 阶段3：汇总搜索结果底表
        _basic_data_merger(keyword="丰盈蓬松洗发水")
        # 阶段4：计算SOV
        _sov_calculator(keyword="丰盈蓬松洗发水")

        return True
  
        
    except Exception as e:
        print(f"❌ 执行过程中出现错误: {e}")
        print("详细错误信息请查看日志")
        return None


def _check_environment() -> bool:
    """检查必要的环境变量"""
    required_env_vars = ['SEO_SUPABASE_URL', 'SEO_SUPABASE_ANON_KEY', 'OPENROUTER_API_KEY']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"错误：缺少必要的环境变量: {', '.join(missing_vars)}")
        print("请创建 .env 文件并设置以下变量：")
        print("SEO_SUPABASE_URL=your_supabase_url")
        print("SEO_SUPABASE_ANON_KEY=your_supabase_anon_key") 
        print("OPENROUTER_API_KEY=your_openrouter_api_key")
        return False
    return True


def _print_startup_info(current_time: str):
    """打印启动信息"""
    print("🚀 开始执行小红书公共舆情分析（批量数据库+分批AI处理）...")
    print(f"📅 分析时间: {current_time}")
    print(BatchConfig.get_config_summary())
    print("="*60)


def _read_database_batch(db_batch_size: int) -> dict:
    """读取数据库批次数据"""
    print("📖 阶段1: 批量数据库读取...")
    db_reader = DatabaseReaderTool()
    raw_data = db_reader._run(batch_size=db_batch_size)
    
    if not raw_data or "没有找到未处理的笔记数据" in raw_data:
        print("⚠️ 没有找到未处理的数据，分析结束")
        return None
    
    try:
        notes_data = json.loads(raw_data)
        total_notes_count = notes_data.get('total_count', 0)
        
        if total_notes_count == 0:
            print("⚠️ 读取到的数据为空，分析结束")
            return None
        
        print(f"✅ 数据读取完成! 共获取 {total_notes_count} 条笔记数据")
        return notes_data
        
    except json.JSONDecodeError as e:
        print(f"❌ 数据解析失败: {e}")
        return None


def _process_all_batches(all_notes_list: list, ai_batch_size: int, current_time: str) -> dict:
    """处理所有批次的AI分析和数据库写入"""
    print("\n" + "="*60)
    print("🤖 阶段2: 分批执行AI内容分析 + 即时数据库写入...")
    
    stats = {
        'total_analysis_results': 0,
        'total_written_results': 0,
        'failed_batches': [],
        'successful_batches': []
    }
    
    db_writer = DatabaseWriterTool()
    total_notes_count = len(all_notes_list)
    
    for batch_index in range(0, total_notes_count, ai_batch_size):
        batch_end = min(batch_index + ai_batch_size, total_notes_count)
        current_batch = all_notes_list[batch_index:batch_end]
        batch_number = (batch_index // ai_batch_size) + 1
        total_batches = (total_notes_count + ai_batch_size - 1) // ai_batch_size
        
        print(f"\n🔄 处理第 {batch_number}/{total_batches} 批 (索引 {batch_index}~{batch_end-1}, 共 {len(current_batch)} 条)")
        
        # 处理单个批次
        batch_result = _process_single_batch(current_batch, batch_number, current_time, db_writer)
        
        # 更新统计信息
        if batch_result['success']:
            stats['successful_batches'].append(batch_result['stats'])
            stats['total_analysis_results'] += batch_result['stats']['analyzed']
            stats['total_written_results'] += batch_result['stats']['written']
        else:
            stats['failed_batches'].append(batch_result['error_info'])
    
    return stats


def _process_single_batch(current_batch: list, batch_number: int, current_time: str, db_writer) -> dict:
    """处理单个批次"""
    try:
        # AI分析阶段
        crew_result = _run_ai_analysis(current_batch, batch_number, current_time)
        if not crew_result:
            return _create_batch_error(batch_number, 'AI分析无返回结果', len(current_batch))
        
        # 解析分析结果
        parse_result = _parse_batch_results(crew_result, batch_number)
        if not parse_result['success']:
            return _create_batch_error(batch_number, parse_result['error'], len(current_batch))
        
        successful_results = parse_result['successful_results']
        failed_results = parse_result['failed_results']
        
        # 数据库写入
        if successful_results:
            written_count = _write_results_to_database(successful_results, batch_number, db_writer)
            return _create_batch_success(batch_number, len(parse_result['all_results']), 
                                       len(successful_results), len(failed_results), written_count)
        else:
            return _create_batch_error(batch_number, '批次内所有结果都解析失败', len(current_batch),
                                     len(successful_results), len(failed_results))
            
    except Exception as e:
        return _create_batch_error(batch_number, str(e), len(current_batch))


def _run_ai_analysis(current_batch: list, batch_number: int, current_time: str):
    """执行AI分析"""
    crew_inputs = {
        'analysis_type': f'小红书笔记情感分析 - 第{batch_number}批',
        'current_date': current_time,
        'batch_size': len(current_batch),
        'notes': current_batch
    }
    
    print(f"📋 开始AI分析: {len(current_batch)} 条笔记")
    return XhsPublicOpinionCrew().crew().kickoff(inputs=crew_inputs)


def _parse_batch_results(crew_result, batch_number: int) -> dict:
    """解析批次结果"""
    try:
        analysis_json = str(crew_result)
        print(f"✅ 第{batch_number}批AI分析完成! 结果长度: {len(analysis_json)} 字符")
        
        # 解析JSON
        if isinstance(crew_result, str):
            batch_results = json.loads(analysis_json)
        else:
            batch_results = json.loads(str(crew_result))
        
        if not isinstance(batch_results, list):
            return {'success': False, 'error': '结果格式不正确（非数组）'}
        
        # 分离成功和失败的结果
        successful_results = []
        failed_results = []
        
        for i, result in enumerate(batch_results):
            if isinstance(result, dict) and result.get('_analysis_failed', False):
                failed_results.append({
                    'index': i,
                    'error_type': result.get('_error_type', 'unknown'),
                    'error_message': result.get('_error_message', 'unknown error')
                })
                logger.warning(f"[Main] 第{batch_number}批第{i+1}条结果分析失败: {result.get('_error_message', 'unknown')}")
            else:
                # 移除失败标记字段
                cleaned_result = {k: v for k, v in result.items() if not k.startswith('_analysis_')}
                successful_results.append(cleaned_result)
        
        print(f"📊 第{batch_number}批解析完成: 总计 {len(batch_results)} 条，成功 {len(successful_results)} 条，失败 {len(failed_results)} 条")
        
        if failed_results:
            _print_failure_details(batch_number, failed_results)
        
        return {
            'success': True,
            'all_results': batch_results,
            'successful_results': successful_results,
            'failed_results': failed_results
        }
        
    except json.JSONDecodeError as e:
        error_msg = f'JSON解析失败: {e}'
        print(f"⚠️ 第{batch_number}批结果解析失败: {e}")
        return {'success': False, 'error': error_msg}


def _print_failure_details(batch_number: int, failed_results: list):
    """打印失败详情"""
    print(f"⚠️ 第{batch_number}批失败详情:")
    for fail_info in failed_results[:3]:  # 只显示前3个失败信息
        print(f"   - 第{fail_info['index']+1}条: {fail_info['error_type']} - {fail_info['error_message'][:50]}...")
    if len(failed_results) > 3:
        print(f"   - 还有 {len(failed_results)-3} 条失败...")


def _write_results_to_database(successful_results: list, batch_number: int, db_writer) -> int:
    """写入结果到数据库"""
    print(f"💾 立即写入第{batch_number}批成功结果到数据库（{len(successful_results)} 条）...")
    
    success_batch_json = json.dumps(successful_results, ensure_ascii=False)
    write_result = db_writer._run(success_batch_json)
    
    if "成功写入" in write_result or "✅" in write_result:
        # 从写入结果中提取成功数量
        success_match = re.search(r'成功写入[：:]\s*(\d+)', write_result)
        written_count = int(success_match.group(1)) if success_match else len(successful_results)
        print(f"✅ 第{batch_number}批数据库写入完成: {written_count} 条")
        return written_count
    else:
        print(f"❌ 第{batch_number}批数据库写入失败")
        return 0


def _create_batch_success(batch_number: int, analyzed: int, successful: int, failed: int, written: int) -> dict:
    """创建批次成功结果"""
    print(f"📊 第{batch_number}批处理结果: AI分析 {analyzed} 条 → 成功 {successful} 条 → 数据库写入 {written} 条")
    return {
        'success': True,
        'stats': {
            'batch': batch_number,
            'analyzed': analyzed,
            'successful_analyzed': successful,
            'failed_analyzed': failed,
            'written': written
        }
    }


def _create_batch_error(batch_number: int, error: str, note_count: int, success_count: int = 0, fail_count: int = 0) -> dict:
    """创建批次错误结果"""
    print(f"❌ 第{batch_number}批处理失败: {error}")
    return {
        'success': False,
        'error_info': {
            'batch': batch_number,
            'error': error,
            'note_count': note_count,
            'success_count': success_count,
            'fail_count': fail_count
        }
    }


def _print_final_statistics(stats: dict, total_notes_count: int, ai_batch_size: int):
    """打印最终统计信息"""
    print(f"\n📊 分批处理完成统计:")
    print(f"✅ 成功批次: {len(stats['successful_batches'])}")
    print(f"❌ 失败批次: {len(stats['failed_batches'])}")
    print(f"📈 总AI分析结果: {stats['total_analysis_results']} 条")
    print(f"💾 总数据库写入: {stats['total_written_results']} 条")
    
    print("\n" + "="*60)
    print("📋 执行总结:")
    print(f"✅ 数据库读取阶段: 完成 (读取 {total_notes_count} 条记录)")
    print(f"✅ AI内容分析阶段: 完成 (分 {(total_notes_count + ai_batch_size - 1) // ai_batch_size} 批处理)")
    print(f"✅ 数据库写入阶段: 完成 (写入 {stats['total_written_results']} 条分析结果)") 
    print(f"⏰ 总执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)


def _create_empty_result(status: str, message: str) -> dict:
    """创建空结果"""
    return {
        "status": status,
        "message": message,
        "timestamp": datetime.now().isoformat()
    }


def _create_success_result(total_notes_count: int, ai_batch_size: int, stats: dict) -> dict:
    """创建成功结果"""
    return {
        "status": "success",
        "data_read_count": total_notes_count,
        "analysis_batches": (total_notes_count + ai_batch_size - 1) // ai_batch_size,
        "analysis_results_count": stats['total_analysis_results'],
        "final_results": {
            "successful_batches": stats['successful_batches'],
            "failed_batches": stats['failed_batches']
        },
        "timestamp": datetime.now().isoformat()
    }

def _basic_data_merger(keyword: str) -> bool:
    """构造基础数据集"""
    data_merger_tool = DataMergerTool()
    res = data_merger_tool._run(keyword)
    if "数据拼接失败" in res:
        return False
    return True

def _sov_calculator(keyword: str) -> bool:
    """计算SOV"""
    print("📖 阶段2: 计算SOV...")
    sov_calculator_tool = SOVCalculatorTool()
    res = sov_calculator_tool._run(keyword, method="simple")
    if "计算失败" in res:
        return False
    return True

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
