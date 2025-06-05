#!/usr/bin/env python
"""
多模态数据批处理器
专门处理包含图片和视频的小红书笔记数据
"""

import json
import sys
import os
from datetime import datetime
from dotenv import load_dotenv

# 添加项目路径
sys.path.append('src')

from xhs_public_opinion.tools import (
    DatabaseReaderTool,
    DatabaseWriterTool,
    MultimodalBrandAnalyzer
)

# 加载环境变量
load_dotenv()

class MultimodalBatchProcessor:
    """多模态批处理器"""
    
    def __init__(self):
        self.db_reader = DatabaseReaderTool()
        self.db_writer = DatabaseWriterTool()
        self.analyzer = MultimodalBrandAnalyzer()
        
    def process_multimodal_data(self, batch_size: int = 50):
        """处理多模态数据"""
        print("🎬 开始多模态数据批处理...")
        print(f"📋 批处理大小: {batch_size}")
        
        try:
            # 读取未处理的数据
            raw_data = self.db_reader._run(batch_size=batch_size)
            if not raw_data or "没有找到未处理的笔记数据" in raw_data:
                print("⚠️ 没有找到未处理的数据")
                return False
            
            # 解析数据
            notes_data = json.loads(raw_data)
            all_notes = notes_data.get('notes', [])
            
            if not all_notes:
                print("⚠️ 数据为空")
                return False
            
            # 分类处理不同类型的内容
            self._classify_and_process(all_notes)
            
            return True
            
        except Exception as e:
            print(f"❌ 处理过程中出现错误: {e}")
            return False
    
    def _classify_and_process(self, all_notes: list):
        """分类和处理不同类型的内容"""
        # 按内容类型分类
        text_only_notes = []      # 纯文本
        image_notes = []          # 包含图片
        video_notes = []          # 包含视频
        complex_notes = []        # 图片+视频
        
        for note in all_notes:
            has_images = note.get('image_list') and len(note.get('image_list', [])) > 0
            has_video = note.get('type') == 'video' and note.get('video_url')
            
            if has_images and has_video:
                complex_notes.append(note)
            elif has_video:
                video_notes.append(note)
            elif has_images:
                image_notes.append(note)
            else:
                text_only_notes.append(note)
        
        # 打印分类统计
        print(f"\n📊 内容分类统计:")
        print(f"   📝 纯文本: {len(text_only_notes)} 条")
        print(f"   🖼️  包含图片: {len(image_notes)} 条")
        print(f"   🎬 包含视频: {len(video_notes)} 条")
        print(f"   🎭 复合内容: {len(complex_notes)} 条")
        
        # 分批处理各类型内容
        total_processed = 0
        
        if text_only_notes:
            print(f"\n🔄 处理纯文本内容...")
            processed = self._process_batch(text_only_notes, "纯文本")
            total_processed += processed
        
        if image_notes:
            print(f"\n🔄 处理图片内容...")
            processed = self._process_batch(image_notes, "图片")
            total_processed += processed
            
        if video_notes:
            print(f"\n🔄 处理视频内容...")
            processed = self._process_batch(video_notes, "视频")
            total_processed += processed
            
        if complex_notes:
            print(f"\n🔄 处理复合内容...")
            processed = self._process_batch(complex_notes, "复合")
            total_processed += processed
        
        print(f"\n✅ 总计处理完成: {total_processed} 条记录")
    
    def _process_batch(self, notes: list, content_type: str) -> int:
        """处理单个批次"""
        if not notes:
            return 0
            
        print(f"   📋 开始处理 {len(notes)} 条{content_type}内容...")
        
        successful_results = []
        failed_count = 0
        
        for i, note in enumerate(notes):
            try:
                # 使用多模态分析器处理
                result = self.analyzer._run(json.dumps(note, ensure_ascii=False))
                parsed_result = json.loads(result)
                
                # 检查是否分析成功
                if parsed_result.get('_analysis_failed', False):
                    failed_count += 1
                    print(f"      ⚠️ 第{i+1}条分析失败: {parsed_result.get('_error_message', 'Unknown')}")
                else:
                    # 添加原始数据ID等信息
                    if 'id' in note:
                        parsed_result['id'] = note['id']
                    successful_results.append(parsed_result)
                    
            except Exception as e:
                failed_count += 1
                print(f"      ❌ 第{i+1}条处理异常: {e}")
        
        # 批量写入成功的结果
        if successful_results:
            try:
                write_result = self.db_writer._run(json.dumps(successful_results, ensure_ascii=False))
                if "成功写入" in write_result:
                    written_count = len(successful_results)
                    print(f"   ✅ {content_type}内容处理完成: 成功 {written_count} 条，失败 {failed_count} 条")
                    return written_count
                else:
                    print(f"   ❌ {content_type}内容数据库写入失败")
                    return 0
            except Exception as e:
                print(f"   ❌ {content_type}内容写入异常: {e}")
                return 0
        else:
            print(f"   ⚠️ {content_type}内容：无成功结果可写入")
            return 0
    
    def get_multimodal_statistics(self):
        """获取多模态数据统计"""
        print("📊 多模态数据统计分析...")
        
        # 这里可以添加从数据库获取统计信息的逻辑
        # 例如：图片数量分布、视频数量分布等
        pass

def main():
    """主函数"""
    print("🚀 多模态数据批处理器启动")
    print("="*60)
    
    # 检查环境变量
    required_env_vars = ['SEO_SUPABASE_URL', 'SEO_SUPABASE_ANON_KEY', 'OPENROUTER_API_KEY']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ 缺少必要的环境变量: {', '.join(missing_vars)}")
        return
    
    # 初始化处理器
    processor = MultimodalBatchProcessor()
    
    # 检查多模态分析器初始化
    if not processor.analyzer.client:
        print("❌ 多模态分析器初始化失败，请检查API配置")
        return
    
    print("✅ 初始化完成，开始处理...")
    print("="*60)
    
    # 处理多模态数据
    batch_size = 100  # 可根据需要调整
    success = processor.process_multimodal_data(batch_size)
    
    if success:
        print("\n" + "="*60)
        print("🎉 多模态数据批处理完成！")
        print(f"⏰ 完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print("\n" + "="*60)
        print("❌ 多模态数据批处理失败")

if __name__ == "__main__":
    main() 