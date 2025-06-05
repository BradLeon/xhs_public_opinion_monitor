import os
import json
from typing import List, Dict, Any, Optional
from crewai.tools import BaseTool
from supabase import create_client, Client
from pydantic import BaseModel, Field
import logging
import re
from dotenv import load_dotenv

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class XHSNote(BaseModel):
    """小红书笔记数据模型"""
    id: int
    note_id: str
    title: Optional[str] = None
    type: Optional[str] = None
    desc: Optional[str] = None
    note_url: Optional[str] = None
    video_url: Optional[str] = None
    image_list: Optional[str] = None
    tag_list: Optional[str] = None
    author_id: Optional[str] = None
    nickname: Optional[str] = None
    brand_list: Optional[str] = None
    spu_list: Optional[str] = None
    emotion_dict: Optional[Dict] = None
    evaluation_dict: Optional[Dict] = None

class SupabaseDatabase:
    """Supabase数据库连接类"""
    
    def __init__(self):
        self.url = os.getenv("SEO_SUPABASE_URL")
        self.key = os.getenv("SEO_SUPABASE_ANON_KEY")
        
        if not self.url or not self.key:
            raise ValueError("请确保设置了 SEO_SUPABASE_URL 和 SEO_SUPABASE_ANON_KEY 环境变量")
        
        self.client: Client = create_client(self.url, self.key)
    
    def get_unprocessed_notes(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取未处理的笔记数据（brand_list为null或空列表字符串）"""
        try:
            response = (
                self.client.table("xhs_note")
                .select("*")
                .or_("brand_list.is.null,brand_list.eq.[]")
                .limit(limit)
                .execute()
            )
            return response.data
        except Exception as e:
            logger.error(f"获取笔记数据失败: {e}")
            return []
    
    def get_note_by_id(self, note_id: str) -> Optional[Dict[str, Any]]:
        """根据note_id获取笔记"""
        try:
            response = (
                self.client.table("xhs_note")
                .select("*")
                .eq("note_id", note_id)
                .execute()
            )
            return response.data[0] if response.data else None
        except Exception as e:
            logger.error(f"获取笔记失败: {e}")
            return None
    
    def update_analysis_result(self, note_id: str, analysis_result: Dict[str, Any]) -> bool:
        """更新分析结果到数据库"""
        try:
            update_data = {}
            
            if "brand_list" in analysis_result:
                update_data["brand_list"] = analysis_result["brand_list"]
            
            if "spu_list" in analysis_result:
                update_data["spu_list"] = analysis_result["spu_list"]
            
            if "emotion_dict" in analysis_result:
                update_data["emotion_dict"] = analysis_result["emotion_dict"]
            
            if "evaluation_dict" in analysis_result:
                update_data["evaluation_dict"] = analysis_result["evaluation_dict"]
            
            # 检查是否有数据需要更新
            if not update_data:
                logger.warning(f"笔记ID {note_id}: 没有有效的分析结果需要更新")
                return False
            
            logger.info(f"准备更新笔记ID {note_id}，更新字段: {list(update_data.keys())}")
            
            response = (
                self.client.table("xhs_note")
                .update(update_data)
                .eq("note_id", note_id)
                .execute()
            )
            
            # 详细检查响应状态
            logger.info(f"笔记ID {note_id} - Supabase响应状态: {response.status_code if hasattr(response, 'status_code') else 'N/A'}")
            logger.info(f"笔记ID {note_id} - 响应数据: {response.data}")
            
            # 检查是否有错误
            if hasattr(response, 'error') and response.error:
                logger.error(f"笔记ID {note_id} - Supabase错误: {response.error}")
                return False
            
            # 检查影响行数
            if response.data is None:
                logger.error(f"笔记ID {note_id} - 更新失败: 响应数据为空")
                return False
            
            if isinstance(response.data, list):
                affected_rows = len(response.data)
                if affected_rows == 0:
                    logger.error(f"笔记ID {note_id} - 更新失败: 没有找到匹配的记录")
                    return False
                elif affected_rows > 1:
                    logger.warning(f"笔记ID {note_id} - 异常: 更新了多条记录({affected_rows}条)")
                
                logger.info(f"笔记ID {note_id} - 成功更新 {affected_rows} 条记录")
                logger.info(f"笔记ID {note_id} - 更新后数据: {response.data[0] if response.data else 'N/A'}")
            
            logger.info(f"✅ 成功更新笔记ID {note_id} 的分析结果")
            return True
            
        except Exception as e:
            logger.error(f"❌ 更新笔记:{note_id} 分析结果失败: {e}")
            logger.error(f"错误详情: {type(e).__name__}: {str(e)}")
            return False

class DatabaseReaderTool(BaseTool):
    """数据库读取工具"""
    name: str = "database_reader"
    description: str = "从Supabase数据库读取小红书笔记数据，支持指定读取数量限制"
    
    def _run(self,  batch_size: str = "") -> str:
        """读取未处理的笔记数据"""
        try:
      
            logger.info(f"[DatabaseReaderTool] 准备读取 {batch_size} 条未处理的笔记数据")
            
            db = SupabaseDatabase()
            notes = db.get_unprocessed_notes(batch_size)
            
            if not notes:
                return "没有找到未处理的笔记数据"
            
            result = {
                "total_count": len(notes),
                "requested_limit": batch_size,
                "notes": notes
            }

            res = json.dumps(result, ensure_ascii=False, indent=2)
            logger.info(f"[DatabaseReaderTool] 读取数据成功，实际获取 {len(notes)} 条记录")

            return res
            
        except Exception as e:
            logger.error(f"读取数据失败: {e}")
            return f"读取数据失败: {str(e)}"

class DatabaseWriterTool(BaseTool):
    """数据库写入工具"""
    name: str = "database_writer"
    description: str = """批量将分析结果写入Supabase数据库。
    输入参数：analysis_results - 包含分析结果的JSON字符串，格式为：
    [{"note_id": "xxx", "analysis_result": {...}}, {"note_id": "yyy", "analysis_result": {...}}]
    直接传入JSON数组格式的字符串即可，无需额外转义。"""
    
    def _run(self, analysis_results: str) -> str:
        """批量写入分析结果"""
        try:
            db = SupabaseDatabase()
            
            logger.info(f"[DatabaseWriterTool] 开始处理分析结果")
            logger.info(f"[DatabaseWriterTool] 接收到的分析结果长度: {len(analysis_results)}")
            # logger.info(f"[DatabaseWriterTool] 分析结果内容预览: {analysis_results[:500]}")
            
            # 清理输入 - 去除可能的markdown代码块格式
            cleaned_input = analysis_results.strip()
            
            # 去除可能的```json和```包装
            if cleaned_input.startswith('```json'):
                cleaned_input = cleaned_input[7:]
            if cleaned_input.startswith('```'):
                cleaned_input = cleaned_input[3:]
            if cleaned_input.endswith('```'):
                cleaned_input = cleaned_input[:-3]
            
            cleaned_input = cleaned_input.strip()
            logger.info(f"[DatabaseWriterTool] 清理后的内容: {cleaned_input[:200]}...")
            
            # 解析JSON
            try:
                if not cleaned_input.startswith('['):
                    # 如果不是数组格式，尝试将其包装为数组
                    if cleaned_input.startswith('{'):
                        cleaned_input = f"[{cleaned_input}]"
                        logger.info(f"[DatabaseWriterTool] 单个对象已包装为数组")
                
                results_list = json.loads(cleaned_input)
                
                if not isinstance(results_list, list):
                    return f"❌ 分析结果格式错误，期望数组格式，实际收到: {type(results_list)}"
                    
                logger.info(f"[DatabaseWriterTool] JSON解析成功，包含 {len(results_list)} 条记录")
                
            except json.JSONDecodeError as e:
                logger.error(f"[DatabaseWriterTool] JSON解析失败: {e}")
                logger.error(f"[DatabaseWriterTool] 错误位置: 字符 {e.pos}")
                logger.error(f"[DatabaseWriterTool] 错误附近内容: {cleaned_input[max(0, e.pos-50):e.pos+50]}")
                
                # 尝试自动修复常见的JSON问题
                logger.info(f"[DatabaseWriterTool] 尝试自动修复JSON格式问题...")
                fixed_input = self._attempt_json_fix(cleaned_input, e.pos)
                
                if fixed_input:
                    try:
                        results_list = json.loads(fixed_input)
                        if isinstance(results_list, list):
                            logger.info(f"[DatabaseWriterTool] ✅ JSON修复成功，包含 {len(results_list)} 条记录")
                        else:
                            logger.error(f"[DatabaseWriterTool] ❌ 修复后的数据不是数组格式")
                            return f"❌ JSON修复失败: 数据不是数组格式"
                    except json.JSONDecodeError as fix_error:
                        logger.error(f"[DatabaseWriterTool] ❌ JSON修复失败: {fix_error}")
                        return f"❌ JSON解析错误: {e} 在位置 {e.pos}，自动修复也失败了"
                else:
                    return f"❌ JSON解析错误: {e} 在位置 {e.pos}，无法自动修复"
            
            total_count = len(results_list)
            success_count = 0
            failed_count = 0
            success_ids = []
            failed_records = []
            
            logger.info(f"[DatabaseWriterTool] 开始写入 {total_count} 条记录")
            
            for i, record in enumerate(results_list):
                try:
                    # 验证记录格式
                    if not isinstance(record, dict):
                        failed_count += 1
                        failed_records.append(f"记录{i+1}: 不是字典格式")
                        continue
                    
                    if 'note_id' not in record or 'analysis_result' not in record:
                        failed_count += 1
                        failed_records.append(f"记录{i+1}: 缺少必要字段")
                        continue
                    
                    note_id = record['note_id']
                    analysis_result = record['analysis_result']
                    
                    logger.info(f"[DatabaseWriterTool] 处理第{i+1}条记录: {note_id}")
                    
                    # 写入数据库
                    success = db.update_analysis_result(note_id, analysis_result)
                    
                    if success:
                        success_count += 1
                        success_ids.append(note_id)
                        logger.info(f"[DatabaseWriterTool] ✅ 成功写入: {note_id}")
                    else:
                        failed_count += 1
                        failed_records.append(f"{note_id}: 数据库写入失败")
                        logger.error(f"[DatabaseWriterTool] ❌ 写入失败: {note_id}")
                        
                except Exception as e:
                    failed_count += 1
                    note_id = record.get('note_id', f'记录{i+1}')
                    failed_records.append(f"{note_id}: {str(e)}")
                    logger.error(f"[DatabaseWriterTool] ❌ 处理记录{i+1}时出错: {e}")
            
            # 生成报告
            report = self._create_error_report(success_count, failed_records, total_count)
            
            logger.info(f"[DatabaseWriterTool] 写入操作完成: 成功{success_count}条，失败{failed_count}条")
            return report.strip()
            
        except Exception as e:
            error_msg = f"❌ 数据库写入工具执行失败: {str(e)}"
            logger.error(f"[DatabaseWriterTool] {error_msg}")
            return error_msg 

    def _create_error_report(self, processed: int, failed_records: List[Dict], total: int) -> str:
        """创建错误报告"""
        if total == 0:
            return "📊 数据库写入统计报告:\n🔢 总记录数: 0\n⚠️ 没有数据需要处理"
        
        success_count = processed - len(failed_records)
        success_rate = (success_count / total) * 100 if total > 0 else 0
        
        report = f"""📊 数据库写入统计报告:
🔢 总记录数: {total}
✅ 成功写入: {success_count} 条 ({success_rate:.1f}%)
❌ 写入失败: {len(failed_records)} 条

📋 处理详情:
"""
        
        if failed_records:
            report += "⚠️ 失败记录详情:\n"
            for i, record in enumerate(failed_records[:5], 1):  # 只显示前5个失败记录
                report += f"  {i}. note_id: {record.get('note_id', 'unknown')} - 错误: {record.get('error', 'unknown')}\n"
            
            if len(failed_records) > 5:
                report += f"  ... 还有 {len(failed_records) - 5} 条失败记录\n"
        else:
            report += "🎉 所有记录都成功写入!\n"
        
        return report 

    def _attempt_json_fix(self, input_str: str, error_pos: int) -> Optional[str]:
        """尝试自动修复常见的JSON问题"""
        try:
            logger.info(f"[DatabaseWriterTool] 开始JSON修复，错误位置: {error_pos}")
            
            # 策略1: 移除多余的数据（在最后一个有效JSON结构之后）
            # 查找最后一个完整的JSON对象或数组结束位置
            for i in range(len(input_str) - 1, -1, -1):
                if input_str[i] in ']}':
                    truncated = input_str[:i+1]
                    logger.info(f"[DatabaseWriterTool] 尝试截断到位置 {i+1}")
                    try:
                        json.loads(truncated)
                        logger.info(f"[DatabaseWriterTool] ✅ 截断修复成功")
                        return truncated
                    except json.JSONDecodeError:
                        continue
            
            # 策略2: 如果是"extra data"错误，尝试移除尾部额外内容
            if "Extra data" in str(error_pos) or error_pos < len(input_str):
                # 从错误位置之前开始向后查找完整的JSON结构
                for end_pos in range(error_pos, len(input_str)):
                    if input_str[end_pos] in ']}':
                        candidate = input_str[:end_pos+1]
                        try:
                            json.loads(candidate)
                            logger.info(f"[DatabaseWriterTool] ✅ Extra data修复成功，截断到位置 {end_pos+1}")
                            return candidate
                        except json.JSONDecodeError:
                            continue
            
            # 策略3: 尝试移除常见的尾部干扰内容
            patterns_to_remove = [
                r'\n\s*$',  # 尾部换行和空白
                r'[^}\]]*$',  # 尾部非JSON字符
                r'\s*[^}\]]+$',  # 尾部空白和其他字符
            ]
            
            for pattern in patterns_to_remove:
                cleaned = re.sub(pattern, '', input_str, flags=re.MULTILINE)
                if cleaned != input_str:
                    try:
                        json.loads(cleaned)
                        logger.info(f"[DatabaseWriterTool] ✅ 正则清理修复成功")
                        return cleaned
                    except json.JSONDecodeError:
                        continue
            
            logger.info(f"[DatabaseWriterTool] ❌ 所有修复策略都失败了")
            return None
            
        except Exception as e:
            logger.error(f"[DatabaseWriterTool] JSON修复过程出错: {e}")
            return None