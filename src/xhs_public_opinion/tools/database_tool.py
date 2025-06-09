import os
import json
from typing import Dict, Any, Optional
from crewai.tools import BaseTool
from supabase import create_client
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class XHSNote():
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
        
        self.client = create_client(self.url, self.key)
    
    def get_unprocessed_notes(self, limit: int = 10) -> list:
        """获取未处理的笔记数据"""
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
    
    def get_unprocessed_notes_by_ids(self, note_ids: list) -> list:
        """根据note_id列表获取未处理的笔记数据（只返回brand_list为空的记录）"""
        try:
            if not note_ids:
                return []
            
            response = (
                self.client.table("xhs_note")
                .select("*")
                .in_("note_id", note_ids)
                .or_("brand_list.is.null,brand_list.eq.[]")  # 只获取未处理的笔记
                .execute()
            )
            return response.data
        except Exception as e:
            logger.error(f"根据note_id列表获取未处理笔记数据失败: {e}")
            return []
    
    def update_analysis_result(self, note_id: str, analysis_result: Dict[str, Any]) -> bool:
        """更新分析结果到数据库"""
        try:
            update_data = {
                "brand_list": analysis_result.get("brand_list", []),
                "spu_list": analysis_result.get("spu_list", []),
                "emotion_dict": analysis_result.get("emotion_dict", {}),
                "evaluation_dict": analysis_result.get("evaluation_dict", {}),
            }
            
            response = (
                self.client.table("xhs_note")
                .update(update_data)
                .eq("note_id", note_id)
                .execute()
            )
            
            return bool(response.data)
            
        except Exception as e:
            logger.error(f"更新笔记:{note_id} 分析结果失败: {e}")
            return False

class DatabaseReaderTool(BaseTool):
    """数据库读取工具"""
    name: str = "database_reader"
    description: str = "从Supabase数据库读取小红书笔记数据，支持指定读取数量限制"
    
    def _run(self, batch_size: str = "") -> str:
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

            return json.dumps(result, ensure_ascii=False)
            
        except Exception as e:
            logger.error(f"读取数据失败: {e}")
            return f"读取数据失败: {str(e)}"

class SingleNoteWriterTool(BaseTool):
    """单条笔记分析结果写入工具"""
    name: str = "single_note_writer"
    description: str = "将单条笔记的分析结果写入数据库"

    def _run(self, result_dict: Dict) -> str:
        """写入单条笔记的分析结果"""
        try:
            logger.info("[SingleNoteWriterTool] 开始写入单条分析结果")
            
            note_id = result_dict.get('note_id')
            if not note_id:
                return "❌ 缺少note_id字段"
            
            db = SupabaseDatabase()
            success = db.update_analysis_result(note_id, result_dict)
            
            if success:
                logger.info(f"[SingleNoteWriterTool] ✅ 成功写入笔记分析结果: {note_id}")
                return f"✅ 成功写入笔记分析结果: {note_id}"
            else:
                error_msg = "数据库写入失败"
                logger.error(f"[SingleNoteWriterTool] ❌ {error_msg}")
                return f"❌ {error_msg}"
                
        except Exception as e:
            error_msg = f"写入异常: {str(e)}"
            logger.error(f"[SingleNoteWriterTool] ❌ {error_msg}")
            return f"❌ {error_msg}"

class SpecificNotesReaderTool(BaseTool):
    """特定笔记读取工具"""
    name: str = "specific_notes_reader"
    description: str = "根据note_id列表从Supabase数据库读取特定的小红书笔记数据"
    
    def _run(self, note_ids: list) -> str:
        """读取特定note_id的笔记数据"""
        try:
            logger.info(f"[SpecificNotesReaderTool] 准备读取 {len(note_ids)} 条特定笔记数据")
            
            db = SupabaseDatabase()
            notes = db.get_unprocessed_notes_by_ids(note_ids)
            
            if not notes:
                return "没有找到指定的笔记数据"
            
            result = {
                "total_count": len(notes),
                "requested_count": len(note_ids),
                "notes": notes
            }

            return json.dumps(result, ensure_ascii=False)
            
        except Exception as e:
            logger.error(f"读取特定笔记数据失败: {e}")
            return f"读取特定笔记数据失败: {str(e)}"
        
    