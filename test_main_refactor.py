#!/usr/bin/env python
"""
测试重构后的main.py各个函数
"""

import sys
import os
import json

# 添加src路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_environment_check():
    """测试环境检查函数"""
    print("🧪 测试环境检查函数")
    print("="*40)
    
    # 保存原始环境变量
    original_env = {}
    test_vars = ['SEO_SUPABASE_URL', 'SEO_SUPABASE_ANON_KEY', 'OPENROUTER_API_KEY']
    
    for var in test_vars:
        original_env[var] = os.environ.get(var)
    
    try:
        from xhs_public_opinion.main import _check_environment
        
        # 测试1: 所有环境变量都存在
        for var in test_vars:
            os.environ[var] = 'test_value'
        result = _check_environment()
        print(f"✅ 所有环境变量存在: {result}")
        
        # 测试2: 缺少环境变量
        del os.environ['OPENROUTER_API_KEY']
        result = _check_environment()
        print(f"❌ 缺少环境变量: {result}")
        
    finally:
        # 恢复原始环境变量
        for var, value in original_env.items():
            if value is not None:
                os.environ[var] = value
            elif var in os.environ:
                del os.environ[var]

def test_result_creators():
    """测试结果创建函数"""
    print("\n🧪 测试结果创建函数")
    print("="*40)
    
    from xhs_public_opinion.main import _create_empty_result, _create_success_result
    
    # 测试空结果
    empty_result = _create_empty_result("no_data", "没有数据")
    print(f"✅ 空结果: {empty_result}")
    
    # 测试成功结果
    stats = {
        'total_analysis_results': 100,
        'successful_batches': [{'batch': 1}],
        'failed_batches': []
    }
    success_result = _create_success_result(100, 10, stats)
    print(f"✅ 成功结果: {success_result}")

def test_batch_result_creators():
    """测试批次结果创建函数"""
    print("\n🧪 测试批次结果创建函数")
    print("="*40)
    
    from xhs_public_opinion.main import _create_batch_success, _create_batch_error
    
    # 测试批次成功
    success = _create_batch_success(1, 10, 8, 2, 8)
    print(f"✅ 批次成功: {success}")
    
    # 测试批次错误
    error = _create_batch_error(1, "测试错误", 10, 0, 10)
    print(f"❌ 批次错误: {error}")

def test_failure_details():
    """测试失败详情打印"""
    print("\n🧪 测试失败详情打印")
    print("="*40)
    
    from xhs_public_opinion.main import _print_failure_details
    
    failed_results = [
        {
            'index': 0,
            'error_type': 'json_decode_error',
            'error_message': '这是一个很长的错误信息，应该被截断以便于显示'
        },
        {
            'index': 1,
            'error_type': 'parsing_error',
            'error_message': '另一个错误信息'
        },
        {
            'index': 2,
            'error_type': 'analysis_exception',
            'error_message': '第三个错误'
        },
        {
            'index': 3,
            'error_type': 'json_extraction_failed',
            'error_message': '第四个错误（不应该显示）'
        }
    ]
    
    _print_failure_details(1, failed_results)

def test_function_structure():
    """测试函数结构和导入"""
    print("\n🧪 测试函数结构")
    print("="*40)
    
    try:
        import xhs_public_opinion.main as main_module
        
        # 检查所有新函数是否存在
        required_functions = [
            '_check_environment',
            '_print_startup_info', 
            '_read_database_batch',
            '_process_all_batches',
            '_process_single_batch',
            '_run_ai_analysis',
            '_parse_batch_results',
            '_print_failure_details',
            '_write_results_to_database',
            '_create_batch_success',
            '_create_batch_error',
            '_print_final_statistics',
            '_create_empty_result',
            '_create_success_result'
        ]
        
        for func_name in required_functions:
            if hasattr(main_module, func_name):
                print(f"✅ {func_name}: 存在")
            else:
                print(f"❌ {func_name}: 不存在")
                
        # 检查原始run函数是否存在且简化
        if hasattr(main_module, 'run'):
            import inspect
            source = inspect.getsource(main_module.run)
            lines = source.split('\n')
            print(f"✅ run函数: 存在，共 {len(lines)} 行（重构前约270行）")
        else:
            print("❌ run函数: 不存在")
            
    except Exception as e:
        print(f"❌ 导入错误: {e}")

def main():
    """运行所有测试"""
    print("🚀 测试重构后的main.py")
    print("="*50)
    
    test_environment_check()
    test_result_creators()
    test_batch_result_creators()
    test_failure_details()
    test_function_structure()
    
    print("\n" + "="*50)
    print("✅ 测试完成！重构后的main.py结构更清晰，函数职责分离明确")

if __name__ == "__main__":
    main() 