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
    column_mapping: Dict[str, str] = {}

    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.url = os.getenv("SEO_SUPABASE_URL")
        self.key = os.getenv("SEO_SUPABASE_ANON_KEY")
        
        if not self.url or not self.key:
            raise ValueError("请确保设置了 SEO_SUPABASE_URL 和 SEO_SUPABASE_ANON_KEY 环境变量")
        
        self.client = create_client(self.url, self.key)
        
        # 初始化品牌标准化器
        self.brand_normalizer = get_brand_normalizer()

        self.column_mapping = {
        "keyword": "搜索关键词",
        "rank": "搜索排名",
        "note_id": "笔记ID",
        "type": "笔记类型",
        "title": "笔记标题",
        "desc": "笔记描述",
        "note_url": "笔记URL",
        "author_id": "作者ID",
        "nickname": "作者昵称",
        "last_update_time": "笔记最后更新时间",
        "liked_count": "点赞数",
        "collected_count": "收藏数",
        "comment_count": "评论数",
        "share_count": "分享数",
        "brand_list": "品牌列表",
        "spu_list": "SPU列表",
        "emotion_dict": "情感倾向",
        "evaluation_dict": "高频词",
        "data_crawler_time": "数据采集时间",
    }
        
    
    def _run(self, keyword: str, output_dir_inner: str = "data/export",  output_dir_outer: str = "outputs") -> str:
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
            
            # 2. 合并多账户排序结果
            merged_rankings = self._merge_multi_account_rankings(search_results)
            logger.info(f"[DataMergerTool] 合并后得到 {len(merged_rankings)} 个唯一note_id的排序")
            
            # 3. 提取所有note_id
            note_ids = list(merged_rankings.keys())
            
            logger.info(f"[DataMergerTool] 提取到 {len(note_ids)} 个唯一的note_id")
            
            # 4. 从xhs_note表获取对应的笔记详情
            note_details = self._get_note_details(note_ids)
            
            logger.info(f"[DataMergerTool] 获取到 {len(note_details)} 条笔记详情")
            
            # 5. 数据拼接（使用合并后的排序结果）
            merged_data = self._merge_data_with_rankings(merged_rankings, note_details, keyword)
            
            # 6. 生成CSV文件（只保存前100名）
            csv_path = self._save_to_csv(merged_data, keyword, output_dir_inner, output_dir_outer)
            
            # 7. 生成统计报告（基于前100名数据）
            # 按rank排序并只取前100名用于统计
            sorted_data = sorted(merged_data, key=lambda x: x.get('rank', float('inf')))
            top_100_data = sorted_data[:100]
            stats = self._generate_statistics(top_100_data, keyword)
            logger.info(f"""✅ 数据拼接完成！

📊 统计信息:
- 关键词: {keyword}
- 原始搜索结果记录: {len(search_results)} 条
- 合并后唯一笔记数: {len(note_ids)} 个
- 筛选前100名记录: {len(top_100_data)} 条
- 成功匹配: {stats['matched_count']} 条
- 未匹配: {stats['unmatched_count']} 条
- 输出文件: {csv_path}

📈 数据概览（前100名）:
- 总记录数: {len(top_100_data)}
- 包含品牌信息的记录: {stats['with_brand_count']} 条
- 涉及品牌数: {stats['unique_brands']} 个
- 涉及搜索账户数: {stats['account_count']} 个

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
    
    def _merge_multi_account_rankings(self, search_results: List[Dict]) -> Dict[str, Dict]:
        """
        合并多账户的排序结果
        
        Args:
            search_results: 原始搜索结果列表
            
        Returns:
            Dict[note_id, {merged_rank, final_rank, account_ranks, account_list}]
        """
        # 按note_id分组，收集各账户的排名
        note_rankings = {}
        all_accounts = set()
        
        for result in search_results:
            note_id = result.get('note_id')
            account = result.get('search_account')
            rank = result.get('rank')
            
            if not note_id or not account or rank is None:
                continue
                
            all_accounts.add(account)
            
            if note_id not in note_rankings:
                note_rankings[note_id] = {
                    'account_ranks': {},
                    'search_records': []
                }
            
            note_rankings[note_id]['account_ranks'][account] = rank
            note_rankings[note_id]['search_records'].append(result)
        
        all_accounts = sorted(list(all_accounts))
        account_count = len(all_accounts)
        max_rank = 110  # 搜索结果最大排名
        
        logger.info(f"[DataMergerTool] 发现 {account_count} 个搜索账户: {all_accounts}")
        
        # 计算每个note_id的合并排名
        merged_rankings = {}
        
        for note_id, data in note_rankings.items():
            account_ranks = data['account_ranks']
            
            # 收集各账户的排名，缺失的用None表示
            ranks = []
            for account in all_accounts:
                ranks.append(account_ranks.get(account))
            # 计算合并排名
            merged_rank = self._calculate_merged_rank(ranks, max_rank)
            
            merged_rankings[note_id] = {
                'merged_rank': merged_rank,
                'account_ranks': account_ranks,
                'account_list': all_accounts,
                'search_records': data['search_records']
            } 
        
        # 按合并排名排序，分配最终排名
        sorted_notes = sorted(merged_rankings.items(), key=lambda x: x[1]['merged_rank'])
        
        for final_rank, (note_id, data) in enumerate(sorted_notes, 1):
            merged_rankings[note_id]['final_rank'] = final_rank
        
        # 记录排序结果
        logger.info(f"[DataMergerTool] 排序示例（前5名）:")
        for i, (note_id, data) in enumerate(sorted_notes[:5]):
            account_ranks_str = ', '.join([f"{acc}:{data['account_ranks'].get(acc, 'N/A')}" 
                                         for acc in all_accounts])
            logger.info(f"  {i+1}. {note_id}: 合并排名={data['merged_rank']:.2f}, 各账户排名=[{account_ranks_str}]")
        
        return merged_rankings
    
    def _calculate_merged_rank(self, ranks: List[Optional[int]], max_rank: int = 110) -> float:
        """
        计算合并排名（加权平均 + 惩罚因子算法）
        
        Args:
            ranks: 各账户的排名列表，None表示未出现
            max_rank: 最大排名值
            
        Returns:
            合并后的排名分数
        """
        valid_ranks = [r for r in ranks if r is not None]
        missing_count = len(ranks) - len(valid_ranks)
        
        if not valid_ranks:
            # 完全没有出现的内容，给予最差排名
            return max_rank * 2
        
        # 方法：加权平均 + 惩罚因子
        avg_rank = sum(valid_ranks) / len(valid_ranks)
        
        # 对缺失数据添加惩罚：每缺失一个账户，排名后移15%
        penalty_rate = 0.15
        penalty = missing_count * penalty_rate * avg_rank
        
        merged_rank = avg_rank + penalty
        
        return merged_rank
    
    def _merge_data_with_rankings(self, merged_rankings: Dict, note_details: List[Dict], keyword: str) -> List[Dict]:
        """
        使用合并排序结果拼接数据
        
        Args:
            merged_rankings: 合并排序结果
            note_details: 笔记详情列表
            keyword: 关键词
            
        Returns:
            拼接后的数据列表
        """
        # 将笔记详情转换为字典，以note_id为key
        note_dict = {note['note_id']: note for note in note_details}
        
        merged_data = []
        
        # 按最终排名排序处理
        sorted_rankings = sorted(merged_rankings.items(), key=lambda x: x[1]['final_rank'])
        
        for note_id, ranking_data in sorted_rankings:
            note_detail = note_dict.get(note_id, {})
            
            # 对品牌相关字段进行标准化处理
            normalized_brand_data = self._normalize_brand_fields(note_detail)
            
            # 获取第一个搜索记录作为代表（用于获取基本搜索信息）
            representative_search = ranking_data['search_records'][0] if ranking_data['search_records'] else {}
            
            # 构建账户排名信息字符串
            account_ranks_info = []
            for account in ranking_data['account_list']:
                rank = ranking_data['account_ranks'].get(account)
                account_ranks_info.append(f"{account}:{rank if rank else 'N/A'}")
            
            # 合并记录
            merged_record = {
                # 搜索结果字段（使用代表性记录）
                'search_id': representative_search.get('id'),
                'keyword': keyword,
                #'search_account': 'MERGED',  # 标识为合并结果
                #'account_count': len(ranking_data['account_ranks']),  # 出现在多少个账户中
                'rank': ranking_data['final_rank'],  # 最终排名
                #'merged_rank': round(ranking_data['merged_rank'], 2),  # 合并排名分数
                #'account_ranks': '; '.join(account_ranks_info),  # 各账户排名详情
                'note_id': note_id,
                
                # 笔记详情字段
                'title': note_detail.get('title'),
                'type': note_detail.get('type'),
                'desc': note_detail.get('desc'),
                'note_url': note_detail.get('note_url'),
                'video_url': note_detail.get('video_url'),
                'image_list': note_detail.get('image_list'),
                'tag_list': note_detail.get('tag_list'),
                'author_id': note_detail.get('author_id'),
                'nickname': note_detail.get('nickname'),
                'last_update_time': note_detail.get('last_update_time'),
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
                'data_crawler_time': representative_search.get('created_at'),
        
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
    
    def _save_to_csv(self, data: List[Dict], keyword: str, output_dir_inner: str, output_dir_outer: str) -> str:
        """保存数据到CSV文件（只保存前100名）"""
        
        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d")
        output_dir_inner = output_dir_inner + "/" + keyword 
        output_dir_outer = output_dir_outer + "/" + keyword 
        
        # 确保输出目录存在
        os.makedirs(output_dir_inner, exist_ok=True)
        os.makedirs(output_dir_outer, exist_ok=True)
        
        # 按rank排序并只取前100名
        sorted_data = sorted(data, key=lambda x: x.get('rank', float('inf')))
        top_100_data = sorted_data[:100]
        
        logger.info(f"[DataMergerTool] 原始数据{len(data)}条，筛选前100名后为{len(top_100_data)}条")
        
        filename = f"merged_data_{timestamp}.csv"
        filepath = os.path.join(output_dir_inner, filename)
        
        # 转换为DataFrame并保存
        df = pd.DataFrame(top_100_data)
        
        # 处理JSON字段，转换为字符串
        json_columns = ['image_list', 'tag_list', 'brand_list', 'spu_list', 'emotion_dict', 'evaluation_dict']
        for col in json_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if x else '')
        
        # 生成对外输出的中文CSV文件
        outer_filename = f"basic_data_{timestamp}.csv"
        outer_filepath = os.path.join(output_dir_outer, outer_filename)
        
        # 筛选column_mapping中存在的列并重命名为中文
        available_columns = [col for col in self.column_mapping.keys() if col in df.columns]
        df_chinese = df[available_columns].copy()
        df_chinese.rename(columns=self.column_mapping, inplace=True)

        # 保存CSV
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        df_chinese.to_csv(outer_filepath, index=False, encoding='utf-8-sig')
        
        logger.info(f"[DataMergerTool] 内部数据已保存到: {filepath}")
        logger.info(f"[DataMergerTool] 对外数据已保存到: {outer_filepath}")
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
        
        # 计算平均合并排名
        merged_ranks = [record['merged_rank'] for record in merged_data if record.get('merged_rank') is not None]
        avg_merged_rank = sum(merged_ranks) / len(merged_ranks) if merged_ranks else 0
        
        # 计算账户统计
        account_counts = [record['account_count'] for record in merged_data if record.get('account_count') is not None]
        avg_account_count = sum(account_counts) / len(account_counts) if account_counts else 0
        max_account_count = max(account_counts) if account_counts else 0
        
        return {
            'matched_count': matched_count,
            'unmatched_count': unmatched_count,
            'with_brand_count': with_brand_count,
            'unique_brands': len(all_brands),
            'avg_merged_rank': avg_merged_rank,
            'account_count': max_account_count,  # 总共涉及的账户数
            'avg_account_per_note': avg_account_count  # 平均每个笔记出现在多少个账户中
        } 