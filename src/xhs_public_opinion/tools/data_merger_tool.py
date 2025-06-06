import os
import json
import pandas as pd
from typing import List, Dict, Any, Optional
from crewai.tools import BaseTool
from supabase import create_client, Client
import logging
from datetime import datetime
from dotenv import load_dotenv
from .brand_normalizer import get_brand_normalizer
from .brand_normalizer import BrandNormalizer

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataMergerTool(BaseTool):
    """数据拼接工具 - 将搜索结果表和笔记详情表连接生成宽表"""
    name: str = "data_merger"
    description: str = "根据指定关键词，将xhs_search_result和xhs_note表根据note_id连接，生成CSV宽表文件"
    
    # 声明Pydantic字段
    url: Optional[str] = None
    key: Optional[str] = None
    client: Optional[Client] = None
    brand_normalizer: Optional[BrandNormalizer] = None
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.url = os.getenv("SEO_SUPABASE_URL")
        self.key = os.getenv("SEO_SUPABASE_ANON_KEY")
        
        if not self.url or not self.key:
            raise ValueError("请确保设置了 SEO_SUPABASE_URL 和 SEO_SUPABASE_ANON_KEY 环境变量")
        
        self.client = create_client(self.url, self.key)
        
        # 初始化品牌标准化器
        self.brand_normalizer = get_brand_normalizer()
    
    def _run(self, keyword: str, output_dir: str = "data/export") -> str:
        """
        执行数据拼接任务
        
        Args:
            keyword: 要查询的关键词
            output_dir: 输出目录，默认为data/export
            
        Returns:
            返回操作结果信息
        """
        try:
            logger.info(f"[DataMergerTool] 开始为关键词 '{keyword}' 进行数据拼接")
            
            # 1. 从xhs_search_result表获取指定关键词的搜索结果
            search_results = self._get_search_results(keyword)
            if not search_results:
                return f"关键词 '{keyword}' 没有找到搜索结果数据"
            
            logger.info(f"[DataMergerTool] 获取到 {len(search_results)} 条搜索结果")
            
            # 2. 提取所有note_id
            note_ids = [result['note_id'] for result in search_results if result.get('note_id')]
            unique_note_ids = list(set(note_ids))  # 去重
            
            logger.info(f"[DataMergerTool] 提取到 {len(unique_note_ids)} 个唯一的note_id")
            
            # 3. 从xhs_note表获取对应的笔记详情
            note_details = self._get_note_details(unique_note_ids)
            
            logger.info(f"[DataMergerTool] 获取到 {len(note_details)} 条笔记详情")
            
            # 4. 数据拼接
            merged_data = self._merge_data(search_results, note_details)
            
            # 5. 生成CSV文件
            csv_path = self._save_to_csv(merged_data, keyword, output_dir)
            
            # 6. 生成统计报告
            stats = self._generate_statistics(merged_data, keyword)
            logger.info(f"""✅ 数据拼接完成！

📊 统计信息:
- 关键词: {keyword}
- 搜索结果记录: {len(search_results)} 条
- 唯一笔记数: {len(unique_note_ids)} 个
- 成功匹配: {stats['matched_count']} 条
- 未匹配: {stats['unmatched_count']} 条
- 输出文件: {csv_path}

📈 数据概览:
- 总记录数: {len(merged_data)}
- 包含品牌信息的记录: {stats['with_brand_count']} 条
- 涉及品牌数: {stats['unique_brands']} 个
- 平均搜索排名: {stats['avg_rank']:.2f}

文件已保存到: {csv_path}""")
            

            return csv_path
            
        except Exception as e:
            logger.error(f"[DataMergerTool] 数据拼接失败: {e}")
            return f"数据拼接失败: {str(e)}"
    
    def _get_search_results(self, keyword: str) -> List[Dict[str, Any]]:
        """从xhs_search_result表获取指定关键词的搜索结果"""
        try:
            response = (
                self.client.table("xhs_search_result")
                .select("*")
                .eq("keyword", keyword)
                .order("rank", desc=False)  # 按排名升序排列
                .execute()
            )
            return response.data
        except Exception as e:
            logger.error(f"获取搜索结果失败: {e}")
            return []
    
    def _get_note_details(self, note_ids: List[str]) -> List[Dict[str, Any]]:
        """从xhs_note表获取指定note_id的笔记详情"""
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
    
    def _merge_data(self, search_results: List[Dict], note_details: List[Dict]) -> List[Dict]:
        """拼接搜索结果和笔记详情数据"""
        # 将笔记详情转换为字典，以note_id为key
        note_dict = {note['note_id']: note for note in note_details}
        
        merged_data = []
        
        for search_record in search_results:
            note_id = search_record.get('note_id')
            note_detail = note_dict.get(note_id, {})
            
            # 对品牌相关字段进行标准化处理
            normalized_brand_data = self._normalize_brand_fields(note_detail)
            
            # 合并记录
            merged_record = {
                # 搜索结果字段
                'search_id': search_record.get('id'),
                'keyword': search_record.get('keyword'),
                'search_account': search_record.get('search_account'),
                'rank': search_record.get('rank'),
                'note_id': note_id,
                
                # 笔记详情字段
                'note_table_id': note_detail.get('id'),
                'title': note_detail.get('title'),
                'type': note_detail.get('type'),
                'desc': note_detail.get('desc'),
                'note_url': note_detail.get('note_url'),
                'video_url': note_detail.get('video_url'),
                'image_list': note_detail.get('image_list'),
                'tag_list': note_detail.get('tag_list'),
                'author_id': note_detail.get('author_id'),
                'nickname': note_detail.get('nickname'),
                'liked_count': note_detail.get('liked_count'),
                'collected_count': note_detail.get('collected_count'),
                'comment_count': note_detail.get('comment_count'),
                'share_count': note_detail.get('share_count'),
                
                # 使用标准化后的品牌数据
                'brand_list': normalized_brand_data['brand_list'],
                'spu_list': note_detail.get('spu_list'),
                'emotion_dict': normalized_brand_data['emotion_dict'],
                'evaluation_dict': normalized_brand_data['evaluation_dict'],
                
                # 数据状态标识
                'has_note_detail': bool(note_detail),
                'has_brand_info': self._has_valid_brand_info(normalized_brand_data['brand_list']),
            }
            
            merged_data.append(merged_record)
        
        return merged_data
    
    def _normalize_brand_fields(self, note_detail: Dict) -> Dict:
        """标准化品牌相关字段"""
        result = {
            'brand_list': note_detail.get('brand_list'),
            'emotion_dict': note_detail.get('emotion_dict'),
            'evaluation_dict': note_detail.get('evaluation_dict')
        }
        
        if not note_detail:
            return result
        
        try:
            # 1. 标准化品牌列表
            brand_list = self._parse_json_field(note_detail.get('brand_list'))
            if isinstance(brand_list, list) and brand_list:
                normalized_brands = []
                brand_mapping = {}  # 原始品牌名 -> 标准化品牌名的映射
                
                for original_brand in brand_list:
                    if original_brand and isinstance(original_brand, str):
                        normalized_brand = self.brand_normalizer.normalize_brand_name(original_brand.strip())
                        if normalized_brand:
                            normalized_brands.append(normalized_brand)
                            brand_mapping[original_brand] = normalized_brand
                
                # 去重但保持顺序
                seen = set()
                unique_normalized_brands = []
                for brand in normalized_brands:
                    if brand not in seen:
                        unique_normalized_brands.append(brand)
                        seen.add(brand)
                
                result['brand_list'] = unique_normalized_brands
                
                # 2. 更新情感字典中的品牌名
                emotion_dict = self._parse_json_field(note_detail.get('emotion_dict'))
                if isinstance(emotion_dict, dict) and emotion_dict:
                    normalized_emotion_dict = {}
                    for original_brand, emotion_info in emotion_dict.items():
                        # 查找对应的标准化品牌名
                        normalized_brand = brand_mapping.get(original_brand, 
                                                           self.brand_normalizer.normalize_brand_name(original_brand))
                        if normalized_brand:
                            normalized_emotion_dict[normalized_brand] = emotion_info
                    result['emotion_dict'] = normalized_emotion_dict
                
                # 3. 更新评价字典中的品牌名
                evaluation_dict = self._parse_json_field(note_detail.get('evaluation_dict'))
                if isinstance(evaluation_dict, dict) and evaluation_dict:
                    normalized_evaluation_dict = {}
                    for original_brand, evaluation_info in evaluation_dict.items():
                        # 查找对应的标准化品牌名
                        normalized_brand = brand_mapping.get(original_brand,
                                                           self.brand_normalizer.normalize_brand_name(original_brand))
                        if normalized_brand:
                            normalized_evaluation_dict[normalized_brand] = evaluation_info
                    result['evaluation_dict'] = normalized_evaluation_dict
                
                # 记录标准化结果
                if brand_mapping:
                    original_brands = list(brand_mapping.keys())
                    normalized_brands = list(brand_mapping.values())
                    logger.debug(f"[DataMergerTool] 品牌标准化: {original_brands} -> {normalized_brands}")
            
        except Exception as e:
            logger.warning(f"[DataMergerTool] 品牌标准化失败: {e}")
            # 如果标准化失败，返回原始数据
            pass
        
        return result
    
    def _parse_json_field(self, field_value: Any) -> Any:
        """安全解析JSON字段"""
        if isinstance(field_value, str):
            try:
                return json.loads(field_value)
            except (json.JSONDecodeError, TypeError):
                return field_value
        return field_value
    
    def _has_valid_brand_info(self, brand_list) -> bool:
        """判断是否有有效的品牌信息"""
        if not brand_list:  # None, '', False, 0 等falsy值
            return False
        
        # 如果是字符串，尝试解析为JSON
        if isinstance(brand_list, str):
            try:
                parsed_list = json.loads(brand_list)
                return isinstance(parsed_list, list) and len(parsed_list) > 0
            except (json.JSONDecodeError, TypeError):
                return False
        
        # 如果已经是列表，检查是否非空
        if isinstance(brand_list, list):
            return len(brand_list) > 0
        
        # 其他情况返回False
        return False
    
    def _save_to_csv(self, data: List[Dict], keyword: str, output_dir: str) -> str:
        """保存数据到CSV文件"""
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"merged_data_{keyword}_{timestamp}.csv"
        filepath = os.path.join(output_dir, filename)
        
        # 转换为DataFrame并保存
        df = pd.DataFrame(data)
        
        # 处理JSON字段，转换为字符串
        json_columns = ['image_list', 'tag_list', 'brand_list', 'spu_list', 'emotion_dict', 'evaluation_dict']
        for col in json_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if x else '')
        
        # 保存CSV
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        
        logger.info(f"[DataMergerTool] 数据已保存到: {filepath}")
        return filepath
    
    def _generate_statistics(self, merged_data: List[Dict], keyword: str) -> Dict[str, Any]:
        """生成统计信息"""
        total_count = len(merged_data)
        matched_count = sum(1 for record in merged_data if record['has_note_detail'])
        unmatched_count = total_count - matched_count
        with_brand_count = sum(1 for record in merged_data if record['has_brand_info'])
        
        # 计算唯一品牌数
        all_brands = set()
        for record in merged_data:
            if record.get('brand_list'):
                try:
                    brands = record['brand_list'] if isinstance(record['brand_list'], list) else json.loads(record['brand_list'] or '[]')
                    all_brands.update(brands)
                except:
                    pass
        
        # 计算平均排名
        ranks = [record['rank'] for record in merged_data if record.get('rank') is not None]
        avg_rank = sum(ranks) / len(ranks) if ranks else 0
        
        return {
            'matched_count': matched_count,
            'unmatched_count': unmatched_count,
            'with_brand_count': with_brand_count,
            'unique_brands': len(all_brands),
            'avg_rank': avg_rank
        } 