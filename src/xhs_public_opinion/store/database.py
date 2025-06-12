"""
数据库操作类 - 数据交互层核心
统一管理所有Supabase数据库操作
"""

import os
import json
import pandas as pd
from typing import Dict, Any, Optional, List
from supabase import create_client
import logging
from datetime import datetime, timedelta

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
    """Supabase数据库连接类 - 统一所有数据库操作"""
    
    def __init__(self):
        self.url = os.getenv("SEO_SUPABASE_URL")
        self.key = os.getenv("SEO_SUPABASE_ANON_KEY")
        
        if not self.url or not self.key:
            logger.warning("Supabase环境变量未设置，数据库功能将受限")
            self.client = None
        else:
            self.client = create_client(self.url, self.key)
    
    def is_connected(self) -> bool:
        """检查数据库连接是否可用"""
        return self.client is not None
    
    # ==================== 基础笔记操作 ====================
    
    def get_unprocessed_notes(self, limit: int = 10) -> list:
        """获取未处理的笔记数据"""
        if not self.client:
            logger.error("数据库客户端未初始化")
            return []
            
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
        if not self.client:
            logger.error("数据库客户端未初始化")
            return []
            
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
        if not self.client:
            logger.error("数据库客户端未初始化")
            return False
            
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
            
    # ==================== 搜索结果操作 ====================
    
    def get_search_results_by_keyword(self, keyword: str) -> List[Dict[str, Any]]:
        """根据关键词获取搜索结果（限定本周内的数据）"""
        if not self.client:
            logger.error("数据库客户端未初始化")
            return []
            
        try:
            # 计算本周的开始时间（周一00:00:00）
            today = datetime.now()
            # 计算当前日期是周几（0=周一，6=周日）
            days_since_monday = today.weekday()
            # 计算本周一的日期
            monday = today - timedelta(days=days_since_monday)
            # 设置为周一的00:00:00
            week_start = monday.replace(hour=0, minute=0, second=0, microsecond=0)
            
            logger.info(f"[Database] 查询关键词'{keyword}'本周内的搜索结果，时间范围：{week_start} 至今")
            
            response = (
                self.client.table("xhs_search_result")
                .select("*")
                .eq("keyword", keyword)
                .gte("created_at", week_start.isoformat())  # 大于等于本周一开始时间
                .order("rank", desc=False)  # 按排名升序排列
                .execute()
            )
            
            logger.info(f"[Database] 查询到 {len(response.data)} 条本周内的搜索结果")
            return response.data
        except Exception as e:
            logger.error(f"获取搜索结果失败: {e}")
            return []
    
    def get_search_results_by_keyword_with_date_range(
        self, 
        keyword: str, 
        start_date: Optional[datetime] = None, 
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """根据关键词和时间范围获取搜索结果（提供自定义时间范围）
        
        Args:
            keyword: 搜索关键词
            start_date: 开始时间（可选，默认为本周一）
            end_date: 结束时间（可选，默认为当前时间）
        """
        if not self.client:
            logger.error("数据库客户端未初始化")
            return []
            
        try:
            # 如果没有提供开始时间，默认使用本周一
            if start_date is None:
                today = datetime.now()
                days_since_monday = today.weekday()
                monday = today - timedelta(days=days_since_monday)
                start_date = monday.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # 构建查询
            query = (
                self.client.table("xhs_search_result")
                .select("*")
                .eq("keyword", keyword)
                .gte("created_at", start_date.isoformat())
            )
            
            # 如果提供了结束时间，添加结束时间条件
            if end_date is not None:
                query = query.lte("created_at", end_date.isoformat())
            
            response = query.order("rank", desc=False).execute()
            
            logger.info(f"[Database] 查询到 {len(response.data)} 条指定时间范围内的搜索结果")
            return response.data
        except Exception as e:
            logger.error(f"获取搜索结果失败: {e}")
            return []
    
    def get_note_details_by_ids(self, note_ids: List[str]) -> List[Dict[str, Any]]:
        """从xhs_note表获取指定note_id的笔记详情"""
        if not self.client:
            logger.error("数据库客户端未初始化")
            return []
            
        if not note_ids:
            return []
        
        try:
            # Supabase的in操作
            response = (
                self.client.table("xhs_note")
                .select("*")
                .in_("note_id", note_ids)
                .execute()
            )
            return response.data
        except Exception as e:
            logger.error(f"获取笔记详情失败: {e}")
            return []
    
    # ==================== SOV数据库操作 ====================
    
    def get_sov_data_by_keyword(self, keyword: str, tier_limit: Optional[int] = None) -> List[Dict]:
        """从数据库获取SOV数据"""
        if not self.client:
            logger.error("数据库客户端未初始化")
            return []
            
        try:
            query = (
                self.client.table("xhs_keyword_sov_result")
                .select("*")
                .eq("keyword", keyword)
                .order("sov_percentage", desc=True)
            )
            
            if tier_limit:
                query = query.eq("tier_limit", tier_limit)
            
            response = query.execute()
            return response.data
        except Exception as e:
            logger.error(f"获取SOV数据失败: {e}")
            return []
    
    def batch_insert_sov_data(self, data_to_insert: List[Dict]) -> Dict[str, Any]:
        """批量插入SOV数据到数据库"""
        if not self.client:
            return {"success": False, "error": "数据库客户端未初始化"}
            
        if not data_to_insert:
            return {"success": False, "error": "没有数据需要插入"}
        
        try:
            response = (
                self.client.table("xhs_keyword_sov_result")
                .insert(data_to_insert)
                .execute()
            )
            
            if response.data:
                return {"success": True, "inserted_count": len(response.data)}
            else:
                return {"success": True, "inserted_count": len(data_to_insert)}
                
        except Exception as e:
            logger.error(f"批量插入SOV数据失败: {e}")
            return {"success": False, "error": str(e)}
    
    # ==================== 情感分析数据库操作 ====================
    
    def get_sentiment_data_by_keyword_brand(self, keyword: str, brand: Optional[str] = None) -> List[Dict]:
        """从数据库获取情感分析数据"""
        if not self.client:
            logger.error("数据库客户端未初始化")
            return []
            
        try:
            query = (
                self.client.table("xhs_keyword_brand_rank_sentiment_result")
                .select("*")
                .eq("keyword", keyword)
                .order("rank", desc=False)
            )
            
            if brand:
                query = query.eq("brand", brand)
            
            response = query.execute()
            return response.data
        except Exception as e:
            logger.error(f"获取情感分析数据失败: {e}")
            return []
    
    def batch_insert_sentiment_data(self, data_to_insert: List[Dict]) -> Dict[str, Any]:
        """批量插入情感分析数据到数据库"""
        if not self.client:
            return {"success": False, "error": "数据库客户端未初始化"}
            
        if not data_to_insert:
            return {"success": False, "error": "没有数据需要插入"}
        
        try:
            response = (
                self.client.table("xhs_keyword_brand_rank_sentiment_result")
                .insert(data_to_insert)
                .execute()
            )
            
            if response.data:
                return {"success": True, "inserted_count": len(response.data)}
            else:
                return {"success": True, "inserted_count": len(data_to_insert)}
                
        except Exception as e:
            logger.error(f"批量插入情感分析数据失败: {e}")
            return {"success": False, "error": str(e)}
    
    # ==================== 通用数据处理方法 ====================
    
    def safe_int(self, value, default=0):
        """安全转换为整数"""
        try:
            if pd.isna(value) or value == '' or value is None:
                return default
            return int(float(value))
        except (ValueError, TypeError):
            return default
    
    def safe_float(self, value, default=0.0):
        """安全转换为浮点数"""
        try:
            if pd.isna(value) or value == '' or value is None:
                return default
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def safe_str(self, value, default=""):
        """安全转换为字符串"""
        try:
            if pd.isna(value) or value is None:
                return default
            return str(value)
        except (ValueError, TypeError):
            return default

    # ==================== 可视化相关数据库操作 ====================
    
    def get_sov_visualization_data(self, keyword: str, tier_limit: Optional[int] = None, limit: int = 100) -> List[Dict]:
        """获取用于SOV可视化的数据"""
        if not self.client:
            logger.error("数据库客户端未初始化")
            return []
            
        try:
            query = (
                self.client.table("xhs_keyword_sov_result")
                .select("*")
                .eq("keyword", keyword)
                .order("created_at", desc=True)
                .limit(limit)
            )
            
            if tier_limit:
                query = query.eq("tier_limit", tier_limit)
            
            response = query.execute()
            return response.data
        except Exception as e:
            logger.error(f"获取SOV可视化数据失败: {e}")
            return []
    
    def get_sentiment_visualization_data(self, keyword: str, brand: str, max_rank: Optional[int] = None, limit: int = 200) -> List[Dict]:
        """获取用于情感分析可视化的数据"""
        if not self.client:
            logger.error("数据库客户端未初始化")
            return []
            
        try:
            query = (
                self.client.table("xhs_keyword_brand_rank_sentiment_result")
                .select("*")
                .eq("keyword", keyword)
                .eq("brand", brand)
                .order("created_at", desc=True)
                .limit(limit)
            )
            
            if max_rank:
                query = query.lte("rank", max_rank)
            
            response = query.execute()
            return response.data
        except Exception as e:
            logger.error(f"获取情感分析可视化数据失败: {e}")
            return []

    # ==================== 应用层工具方法（取代database_service_tools.py） ====================
    
    def get_unprocessed_notes_json(self, batch_size: str = "10") -> str:
        """
        读取未处理的笔记数据（JSON格式输出）
        取代 DatabaseReaderTool._run()
        """
        try:
            # 处理batch_size参数
            try:
                limit = int(batch_size) if batch_size else 10
            except (ValueError, TypeError):
                limit = 10
            
            logger.info(f"[Database] 准备读取 {limit} 条未处理的笔记数据")
            
            notes = self.get_unprocessed_notes(limit)
            
            if not notes:
                return json.dumps({"error": "没有找到未处理的笔记数据"}, ensure_ascii=False)
            
            result = {
                "total_count": len(notes),
                "requested_limit": limit,
                "notes": notes
            }

            return json.dumps(result, ensure_ascii=False)
            
        except Exception as e:
            logger.error(f"读取数据失败: {e}")
            return json.dumps({"error": f"读取数据失败: {str(e)}"}, ensure_ascii=False)
    
    def update_single_note_analysis_json(self, result_dict: Dict) -> str:
        """
        写入单条笔记的分析结果（JSON格式输出）
        取代 SingleNoteWriterTool._run()
        """
        try:
            logger.info("[Database] 开始写入单条分析结果")
            
            note_id = result_dict.get('note_id')
            if not note_id:
                return json.dumps({"success": False, "message": "❌ 缺少note_id字段"}, ensure_ascii=False)
            
            success = self.update_analysis_result(note_id, result_dict)
            
            if success:
                logger.info(f"[Database] ✅ 成功写入笔记分析结果: {note_id}")
                return json.dumps({
                    "success": True, 
                    "message": f"✅ 成功写入笔记分析结果: {note_id}"
                }, ensure_ascii=False)
            else:
                error_msg = "数据库写入失败"
                logger.error(f"[Database] ❌ {error_msg}")
                return json.dumps({"success": False, "message": f"❌ {error_msg}"}, ensure_ascii=False)
                
        except Exception as e:
            error_msg = f"写入异常: {str(e)}"
            logger.error(f"[Database] ❌ {error_msg}")
            return json.dumps({"success": False, "message": f"❌ {error_msg}"}, ensure_ascii=False)
    
    def get_specific_notes_json(self, note_ids: List[str]) -> str:
        """
        根据note_id列表读取特定笔记数据（JSON格式输出）
        取代 SpecificNotesReaderTool._run()
        """
        try:
            logger.info(f"[Database] 准备读取 {len(note_ids)} 条特定笔记数据")
            
            notes = self.get_unprocessed_notes_by_ids(note_ids)
            
            if not notes:
                return json.dumps({"error": "没有找到指定的笔记数据"}, ensure_ascii=False)
            
            result = {
                "total_count": len(notes),
                "requested_count": len(note_ids),
                "notes": notes
            }

            return json.dumps(result, ensure_ascii=False)
            
        except Exception as e:
            logger.error(f"读取特定笔记数据失败: {e}")
            return json.dumps({"error": f"读取特定笔记数据失败: {str(e)}"}, ensure_ascii=False) 