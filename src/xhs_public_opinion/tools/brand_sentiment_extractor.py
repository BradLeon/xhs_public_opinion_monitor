import os
import json
import pandas as pd
import glob
from typing import Dict, Any, List, Optional
from crewai.tools import BaseTool
import logging
from datetime import datetime
from .brand_normalizer import get_brand_normalizer
from .brand_normalizer import BrandNormalizer

logger = logging.getLogger(__name__)

class BrandSentimentExtractorTool(BaseTool):
    """品牌情感提取助手 - 基于keyword和brand从DataMergerTool的CSV输出中提取情感倾向和高频词"""
    name: str = "brand_sentiment_extractor"
    description: str = "基于指定的keyword和brand，从DataMergerTool生成的CSV文件中提取品牌情感倾向和高频词，输出到CSV文件"
    brand_normalizer: Optional[BrandNormalizer] = None
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 初始化品牌标准化器
        self.brand_normalizer = get_brand_normalizer()
    
    def _run(self, keyword: str, brand: str = "", output_filename: str = "", csv_input_path: str = "") -> str:
        """
        提取指定keyword和brand的笔记详情
        
        Args:
            keyword: 搜索关键词
            brand: 品牌名称（可选，如果为空则提取所有品牌）
            output_filename: 输出文件名（可选）
            csv_input_path: 指定CSV输入文件路径（可选，默认自动查找最新文件）
        
        Returns:
            str: 输出文件路径或错误信息
        """
        try:
            logger.info(f"[BrandSentimentExtractor] 开始提取品牌情感分析数据 - 关键词: {keyword}, 品牌: {brand or '所有品牌'}")
            
            # 1. 读取CSV数据
            df = self._load_csv_data(keyword, csv_input_path)
            if df is None or df.empty:
                return f"❌ 未找到关键词 '{keyword}' 的数据文件"
            
            logger.info(f"[BrandSentimentExtractor] 读取到 {len(df)} 条原始记录")
            
            # 2. 筛选有品牌信息的记录
            df_with_brands = df[df['has_brand_info'] == True].copy()
            logger.info(f"[BrandSentimentExtractor] 筛选出 {len(df_with_brands)} 条包含品牌信息的记录")
            
            if df_with_brands.empty:
                return f"❌ 关键词 '{keyword}' 的数据中没有包含品牌信息的记录"
            
            # 3. 品牌筛选（如果指定了品牌）
            if brand:
                # 标准化输入的品牌名
                normalized_target_brand = self.brand_normalizer.normalize_brand_name(brand)
                logger.info(f"[BrandSentimentExtractor] 标准化目标品牌: '{brand}' -> '{normalized_target_brand}'")
                
                df_filtered = self._filter_by_brand(df_with_brands, normalized_target_brand)
                if df_filtered.empty:
                    return f"❌ 未找到品牌 '{brand}' (标准化为: '{normalized_target_brand}') 的相关记录"
                
                logger.info(f"[BrandSentimentExtractor] 筛选出品牌 '{normalized_target_brand}' 的 {len(df_filtered)} 条记录")
            else:
                df_filtered = df_with_brands
                normalized_target_brand = ""
            
            # 4. 处理数据
            processed_data = self._process_data(df_filtered, normalized_target_brand)
            
            # 5. 生成CSV输出
            output_path = self._generate_csv_output(processed_data, keyword, normalized_target_brand, output_filename)
            
            logger.info(f"✅ [BrandSentimentExtractor] 品牌情感分析完成!")
            logger.info(f"📊 处理统计:")
            logger.info(f"   - 总记录数: {len(processed_data)}")
            logger.info(f"   - 输出文件: {output_path}")
            
            return output_path
            
        except Exception as e:
            error_msg = f"[BrandSentimentExtractor] 处理失败: {str(e)}"
            logger.error(error_msg)
            return error_msg
    
    def _load_csv_data(self, keyword: str, csv_input_path: str = "") -> pd.DataFrame:
        """读取DataMergerTool生成的CSV文件"""
        try:
            if csv_input_path and os.path.exists(csv_input_path):
                # 使用指定的CSV文件路径
                logger.info(f"[BrandSentimentExtractor] 使用指定的CSV文件: {csv_input_path}")
                return pd.read_csv(csv_input_path, encoding='utf-8-sig')
            
        except Exception as e:
            logger.error(f"[BrandSentimentExtractor] 读取CSV文件失败: {e}")
            return pd.DataFrame()
    
    def _filter_by_brand(self, df: pd.DataFrame, target_brand: str) -> pd.DataFrame:
        """根据品牌名筛选记录（使用标准化匹配）"""
        if not target_brand:
            return df
        
        def contains_target_brand(brand_list_str):
            """检查品牌列表中是否包含目标品牌"""
            if not brand_list_str:
                return False
            
            try:
                # 解析品牌列表
                if isinstance(brand_list_str, str):
                    brand_list = json.loads(brand_list_str)
                else:
                    brand_list = brand_list_str
                
                if not isinstance(brand_list, list):
                    return False
                
                # 检查是否有匹配的品牌（已经是标准化后的名称）
                for brand in brand_list:
                    if isinstance(brand, str) and brand.strip() == target_brand:
                        return True
                
                return False
                
            except (json.JSONDecodeError, TypeError, AttributeError):
                return False
        
        # 应用筛选条件
        mask = df['brand_list'].apply(contains_target_brand)
        return df[mask].copy()
    
    def _parse_json_field(self, field_value: Any) -> Any:
        """安全解析JSON字段"""
        # 首先检查是否为pandas的NaN值
        if pd.isna(field_value):
            return []
        
        if isinstance(field_value, str):
            # 处理空字符串
            if not field_value.strip():
                return []
            try:
                return json.loads(field_value)
            except (json.JSONDecodeError, TypeError):
                return field_value
        
        # 如果已经是列表或字典，直接返回
        if isinstance(field_value, (list, dict)):
            return field_value
        
        # 其他情况返回空列表
        return []
    
    def _get_safe_field_value(self, row: pd.Series, field_name: str, default_value: str = "") -> str:
        """安全获取字段值，处理NaN情况"""
        value = row.get(field_name, default_value)
        # 检查是否为NaN（pandas中的空值是float类型）
        if pd.isna(value):
            return default_value
        return str(value)

    def _process_data(self, df: pd.DataFrame, target_brand: str) -> List[Dict]:
        """处理CSV数据，提取关键信息"""
        processed_notes = []
        
        for _, row in df.iterrows():
            try:
                # 基本信息 - 使用安全获取方法
                note_info = {
                    "note_id": self._get_safe_field_value(row, "note_id"),
                    "title": self._get_safe_field_value(row, "title"),
                    "desc": self._get_safe_field_value(row, "desc"),
                    "type": self._get_safe_field_value(row, "type"),
                    "author_id": self._get_safe_field_value(row, "author_id"),
                    "nickname": self._get_safe_field_value(row, "nickname"),
                    "note_url": self._get_safe_field_value(row, "note_url"),
                    "video_url": self._get_safe_field_value(row, "video_url"),
                    
                    # 搜索相关信息
                    "keyword": self._get_safe_field_value(row, "keyword"),
                    "rank": self._get_safe_field_value(row, "rank"),
                    "search_account": self._get_safe_field_value(row, "search_account"),
                    
                    # 互动数据
                    "liked_count": self._get_safe_field_value(row, "liked_count"),
                    "collected_count": self._get_safe_field_value(row, "collected_count"),
                    "comment_count": self._get_safe_field_value(row, "comment_count"),
                    "share_count": self._get_safe_field_value(row, "share_count"),
                }
                
                # 处理品牌列表 - 使用安全获取
                brand_list_raw = self._get_safe_field_value(row, "brand_list", "[]")
                brand_list = self._parse_json_field(brand_list_raw)
                logger.info(f"[BrandSentimentExtractor] 品牌列表原始值: '{brand_list_raw}', 解析后: {brand_list}")
                
                if isinstance(brand_list, list):
                    note_info["all_brands"] = ", ".join(str(brand) for brand in brand_list if brand)
                else:
                    note_info["all_brands"] = ""
                
                # 处理SPU列表 - 使用安全获取
                spu_list_raw = self._get_safe_field_value(row, "spu_list", "[]")
                spu_list = self._parse_json_field(spu_list_raw)
                logger.info(f"[BrandSentimentExtractor] SPU列表原始值: '{spu_list_raw}', 解析后: {spu_list}")
                
                if isinstance(spu_list, list):
                    note_info["all_spus"] = ", ".join(str(spu) for spu in spu_list if spu)
                else:
                    note_info["all_spus"] = ""
                
                # 处理标签列表 - 使用安全获取
                tag_list_raw = self._get_safe_field_value(row, "tag_list", "[]")
                tag_list = self._parse_json_field(tag_list_raw)
                logger.info(f"[BrandSentimentExtractor] 标签列表原始值: '{tag_list_raw}', 解析后: {tag_list}")
                
                if isinstance(tag_list, list):
                    note_info["all_tags"] = ", ".join(str(tag) for tag in tag_list if tag)
                else:
                    note_info["all_tags"] = ""
                
                # 处理情感字典 - 使用安全获取
                emotion_dict_raw = self._get_safe_field_value(row, "emotion_dict", "{}")
                emotion_dict = self._parse_json_field(emotion_dict_raw)
                logger.info(f"[BrandSentimentExtractor] 情感字典原始值: '{emotion_dict_raw}', 解析后: {emotion_dict}")
                
                # 确保brand_list是列表类型
                safe_brand_list = brand_list if isinstance(brand_list, list) else []
                brand_emotion = self._extract_brand_emotion(emotion_dict, target_brand, safe_brand_list)
                note_info.update(brand_emotion)
                
                # 处理评价字典 - 使用安全获取
                evaluation_dict_raw = self._get_safe_field_value(row, "evaluation_dict", "{}")
                evaluation_dict = self._parse_json_field(evaluation_dict_raw)
                logger.info(f"[BrandSentimentExtractor] 评价字典原始值: '{evaluation_dict_raw}', 解析后: {evaluation_dict}")
                
                brand_evaluation = self._extract_brand_evaluation(evaluation_dict, target_brand, safe_brand_list)
                note_info.update(brand_evaluation)
                
                processed_notes.append(note_info)
                
            except Exception as e:
                logger.warning(f"[BrandSentimentExtractor] 处理笔记 {self._get_safe_field_value(row, 'note_id', 'unknown')} 时出错: {e}")
                logger.error(f"[BrandSentimentExtractor] 详细错误信息: {str(e)}", exc_info=True)
                continue
        
        return processed_notes
    
    def _extract_brand_emotion(self, emotion_dict: Dict, target_brand: str, all_brands: List[str]) -> Dict:
        """提取指定品牌的情感倾向"""
        result = {
            "target_brand": target_brand,
            "target_brand_emotion": "",
        }
        
        if not emotion_dict:
            return result
        
        # 如果指定了目标品牌，优先提取目标品牌的情感
        if target_brand:
            for brand_name, emotion_info in emotion_dict.items():
                if target_brand.lower() in brand_name.lower():
                    if isinstance(emotion_info, dict):
                        result["target_brand_emotion"] = emotion_info.get("emotion", "")
                    else:
                        result["target_brand_emotion"] = str(emotion_info)
                    break
        
        
        return result
    
    def _extract_brand_evaluation(self, evaluation_dict: Dict, target_brand: str, all_brands: List[str]) -> Dict:
        """提取指定品牌的评价关键词"""
        result = {
            "target_brand_keywords": "",
        }
        
        if not evaluation_dict:
            return result
        
        # 如果指定了目标品牌，优先提取目标品牌的关键词
        if target_brand:
            for brand_name, keywords in evaluation_dict.items():
                if target_brand.lower() in brand_name.lower():
                    if isinstance(keywords, list):
                        result["target_brand_keywords"] = ", ".join(keywords)
                    elif isinstance(keywords, dict):
                        # 如果是字典格式，提取关键词和频次
                        keyword_list = [f"{k}({v})" for k, v in keywords.items()]
                        result["target_brand_keywords"] = ", ".join(keyword_list)
                    else:
                        result["target_brand_keywords"] = str(keywords)
                    break
        
        
        return result
    
    def _generate_csv_output(self, processed_data: List[Dict], keyword: str, brand: str, output_filename: str) -> str:
        """生成CSV输出文件"""
        # 生成文件名
        if not output_filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            brand_suffix = f"_{brand}" if brand else ""
            output_filename = f"brand_sentiment_{keyword}{brand_suffix}_{timestamp}.csv"
        
        # 确保输出目录存在
        output_dir = "data/export"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, output_filename)
        
        # 创建DataFrame
        df = pd.DataFrame(processed_data)
        
        # 重新排列列的顺序，把重要信息放在前面
        column_order = [
            "keyword", "rank", "search_account",
            "note_id", "note_url", "title", "desc", "type", 
            "author_id", "nickname", 
            "liked_count", "collected_count", "comment_count", "share_count",
            "target_brand", "target_brand_emotion", "target_brand_keywords",
            "all_brand_emotions", "all_brand_keywords",
        ]
        
        # 只保留存在的列
        existing_columns = [col for col in column_order if col in df.columns]
        df = df[existing_columns]
        
        # 输出到CSV
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        logger.info(f"[BrandSentimentExtractor] CSV文件已保存: {output_path}")
        return output_path 