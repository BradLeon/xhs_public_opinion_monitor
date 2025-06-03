#!/usr/bin/env python
"""
SOV分析演示脚本
使用DataMergerTool和SOVCalculatorTool进行品牌声量分析
"""

# 模拟工具调用的示例代码
def demo_data_merger():
    """演示数据拼接工具的使用"""
    print("📊 DataMergerTool 使用示例:")
    print("="*50)
    
    print("""
# 1. 导入工具
from xhs_public_opinion.tools import DataMergerTool

# 2. 创建工具实例
merger = DataMergerTool()

# 3. 执行数据拼接
result = merger._run(
    keyword="面膜",           # 指定关键词
    output_dir="data/export" # 输出目录
)

print(result)
    """)
    
    print("📁 输出文件格式:")
    print("- 文件名: merged_data_{keyword}_{timestamp}.csv")
    print("- 包含字段: search_id, keyword, search_account, rank, note_id,")
    print("           title, type, desc, author_id, nickname,") 
    print("           liked_count, collected_count, comment_count, share_count,")
    print("           brand_list, spu_list, emotion_dict, evaluation_dict")

def demo_sov_calculator():
    """演示SOV计算工具的使用"""
    print("\n📈 SOVCalculatorTool 使用示例:")
    print("="*50)
    
    print("""
# 1. 导入工具
from xhs_public_opinion.tools import SOVCalculatorTool

# 2. 创建工具实例
calculator = SOVCalculatorTool()

# 3. 方法1: 简单计数法SOV
simple_result = calculator._run(
    keyword="面膜",
    method="simple"
)

# 4. 方法2: 排名加权SOV
weighted_result = calculator._run(
    keyword="面膜", 
    method="weighted"
)

# 5. 方法3: 互动量加权SOV
engagement_result = calculator._run(
    keyword="面膜",
    method="engagement"
)
    """)
    
    print("📊 SOV计算方法说明:")
    print("- simple: 基于品牌提及次数的简单占比")
    print("- weighted: 基于搜索排名加权的占比")
    print("- engagement: 基于互动量(点赞+收藏+评论+分享)加权的占比")

def demo_complete_workflow():
    """演示完整的SOV分析工作流"""
    print("\n🔄 完整SOV分析工作流:")
    print("="*50)
    
    print("""
# 完整的分析流程
def analyze_brand_sov(keyword):
    # 步骤1: 数据拼接
    merger = DataMergerTool()
    merge_result = merger._run(keyword=keyword)
    
    if "数据拼接完成" in merge_result:
        # 步骤2: SOV计算
        calculator = SOVCalculatorTool()
        
        # 多种方法计算SOV
        methods = ["simple", "weighted", "engagement"]
        for method in methods:
            sov_result = calculator._run(keyword=keyword, method=method)
            print(f"\\n{method.upper()} SOV结果:")
            print(sov_result)
    else:
        print("数据拼接失败，请检查数据库连接和数据")

# 使用示例
analyze_brand_sov("面膜")
    """)

def demo_data_structure():
    """演示数据结构说明"""
    print("\n📋 数据表结构说明:")
    print("="*50)
    
    print("""
1. xhs_search_result表 (搜索结果表):
   - id: 记录ID
   - keyword: 搜索关键词  
   - search_account: 搜索账户
   - rank: 搜索排名
   - note_id: 笔记ID (关联字段)

2. xhs_note表 (笔记详情表):
   - note_id: 笔记ID (关联字段)
   - title: 笔记标题
   - type: 笔记类型
   - desc: 笔记描述
   - note_url: 笔记链接
   - video_url: 视频链接
   - image_list: 图片列表 (JSON)
   - tag_list: 标签列表 (JSON)
   - author_id: 作者ID
   - nickname: 作者昵称
   - liked_count: 点赞数
   - collected_count: 收藏数
   - comment_count: 评论数
   - share_count: 分享数
   - brand_list: 品牌列表 (JSON) ⭐ SOV计算关键字段
   - spu_list: 产品SPU列表 (JSON)
   - emotion_dict: 情感字典 (JSON)
   - evaluation_dict: 评价字典 (JSON)

3. 拼接后的宽表包含所有字段 + 数据状态标识:
   - has_note_detail: 是否有笔记详情
   - has_brand_info: 是否有品牌信息
    """)

def main():
    """主演示函数"""
    print("🚀 SOV分析工具演示")
    print("🎯 目标: 计算各品牌在指定关键词下的声量占比(Share of Voice)")
    print("="*70)
    
    demo_data_structure()
    demo_data_merger()
    demo_sov_calculator()
    demo_complete_workflow()
    
    print("\n✅ 演示完成!")
    print("\n📖 接下来您可以:")
    print("1. 配置数据库环境变量")
    print("2. 运行 python examples/sov_analysis_example.py 进行实际分析")
    print("3. 或者在您的代码中直接使用这两个工具")

if __name__ == "__main__":
    main() 