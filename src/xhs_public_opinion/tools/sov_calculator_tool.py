import os
import json
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from crewai.tools import BaseTool
import logging
from datetime import datetime
import glob

# 导入品牌标准化工具
from .brand_normalizer import get_brand_normalizer

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SOVCalculatorTool(BaseTool):
    """SOV计算工具 - 计算各品牌在关键词下的声量占比（Share of Voice）"""
    name: str = "sov_calculator"
    description: str = "基于合并后的CSV数据，计算各品牌在指定关键词下的SOV（声量占比）"
    
    def _run(self, keyword: str, data_dir: str = "data/export", method: str = "weighted") -> str:
        """
        计算SOV
        
        Args:
            keyword: 关键词
            data_dir: 数据目录
            method: 计算方法 (simple/weighted/engagement)
                   - simple: 基于笔记数量的简单占比
                   - weighted: 基于搜索排名加权的占比
                   - engagement: 基于互动量加权的占比
                   
        Returns:
            SOV计算结果
        """
        try:
            logger.info(f"[SOVCalculatorTool] 开始计算关键词 '{keyword}' 的SOV，方法: {method}")
            
            # 1. 查找对应的CSV文件
            csv_file = self._find_csv_file(keyword, data_dir)
            if not csv_file:
                return f"未找到关键词 '{keyword}' 的数据文件，请先使用DataMergerTool生成宽表数据"
            
            # 2. 读取CSV数据
            df = pd.read_csv(csv_file)
            logger.info(f"[SOVCalculatorTool] 读取数据文件: {csv_file}, 记录数: {len(df)}")
            
            # 3. 数据预处理
            processed_df = self._preprocess_data(df)
            
            # 4. 根据选择的方法计算SOV
            if method == "simple":
                sov_results = self._calculate_simple_sov(processed_df)
            elif method == "weighted":
                sov_results = self._calculate_weighted_sov(processed_df)
            elif method == "engagement":
                sov_results = self._calculate_engagement_sov(processed_df)
            else:
                return f"不支持的计算方法: {method}。支持的方法: simple, weighted, engagement"
            
            # 5. 保存结果
            result_file = self._save_sov_results(sov_results, keyword, method, data_dir)
            
            # 6. 生成报告
            report = self._generate_sov_report(sov_results, keyword, method, len(processed_df))
            
            return f"""✅ SOV计算完成！

📊 SOV分析报告:
{report}

📁 结果文件: {result_file}
📁 数据源: {csv_file}"""
            
        except Exception as e:
            logger.error(f"[SOVCalculatorTool] SOV计算失败: {e}")
            return f"SOV计算失败: {str(e)}"
    
    def _find_csv_file(self, keyword: str, data_dir: str) -> Optional[str]:
        """查找指定关键词的最新CSV文件"""
        pattern = os.path.join(data_dir, f"merged_data_{keyword}_*.csv")
        files = glob.glob(pattern)
        
        if not files:
            return None
        
        # 返回最新的文件
        latest_file = max(files, key=os.path.getctime)
        logger.info(f"[SOVCalculatorTool] 找到数据文件: {latest_file}")
        return latest_file
    
    def _preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """数据预处理"""
        # 只保留有品牌信息的记录
        df_valid = df[df['has_brand_info'] == True].copy()
        
        logger.info(f"[SOVCalculatorTool] 数据预处理: 原始记录 {len(df)} 条，有品牌信息 {len(df_valid)} 条")
        
        # 处理brand_list字段
        expanded_records = []
        
        for _, row in df_valid.iterrows():
            try:
                brand_list_str = row['brand_list']

                if pd.isna(brand_list_str) or brand_list_str == '':
                    continue
                
                # 解析品牌列表 - 处理可能的双重JSON编码
                if isinstance(brand_list_str, str):
                    brands = json.loads(brand_list_str)
                    
                    # 处理双重编码的情况
                    if isinstance(brands, str):
                        try:
                            brands = json.loads(brands)
                            logger.debug(f"双重解码成功: {brands}")
                        except (json.JSONDecodeError, TypeError):
                            logger.warning(f"双重解码失败: {brands}")
                            continue
                            
                else:
                    brands = brand_list_str
                
                if not isinstance(brands, list):
                    continue
                if len(brands) == 0:
                    continue

                logger.info(f"[SOVCalculatorTool] rank: {row['rank']}，brands:  {brands}")
        
                # 为每个品牌创建一条记录
                for brand in brands:
                    if brand and brand.strip():
                        # 标准化品牌名
                        brand_normalizer = get_brand_normalizer()
                        normalized_brand = brand_normalizer.normalize_brand_name(brand.strip())
                        
                        if normalized_brand:  # 只有标准化后不为空的品牌名才处理
                            # 将pandas Series转换为字典，然后添加brand字段
                            record = row.to_dict()
                            record['brand'] = normalized_brand
                            record['original_brand'] = brand.strip()  # 保留原始品牌名用于调试
                            expanded_records.append(record)
                        
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"解析品牌列表失败: {e}, 数据: {row['brand_list']}")
                continue
        
        if not expanded_records:
            logger.warning("[SOVCalculatorTool] 没有有效的品牌数据")
            return pd.DataFrame()
        
        expanded_df = pd.DataFrame(expanded_records)
        
        # 填充缺失值
        numeric_columns = ['liked_count', 'collected_count', 'comment_count', 'share_count', 'rank']
        for col in numeric_columns:
            if col in expanded_df.columns:
                expanded_df[col] = pd.to_numeric(expanded_df[col], errors='coerce').fillna(0)
        
        logger.info(f"[SOVCalculatorTool] 展开后的品牌记录: {len(expanded_df)} 条，涉及品牌: {expanded_df['brand'].nunique()} 个")
        logger.info(f"[SOVCalculatorTool] 展开后的品牌记录: {expanded_df.head(10)}")

        return expanded_df
    
    def _calculate_simple_sov(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算简单SOV（基于笔记数量）"""

        brand_counts = df['brand'].value_counts()
        total_mentions = len(df)
        
        sov_data = []
        for brand, count in brand_counts.items():
            sov_percentage = (count / total_mentions) * 100
            sov_data.append({
                'brand': brand,
                'mention_count': int(count),
                'sov_percentage': round(sov_percentage, 2),
                'rank': len(sov_data) + 1
            })
        
        return {
            'method': 'simple',
            'total_mentions': total_mentions,
            'unique_brands': len(brand_counts),
            'sov_data': sov_data
        }
    
    def _calculate_weighted_sov(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算加权SOV（基于搜索排名加权）"""
        # 排名权重：排名越靠前权重越高
        # 权重公式：1 / rank (排名第1的权重最高)
        df['rank_weight'] = 1 / (df['rank'] + 1)  # +1避免除零
        
        brand_weighted_scores = df.groupby('brand')['rank_weight'].sum()
        total_weight = df['rank_weight'].sum()
        
        sov_data = []
        for brand, weight in brand_weighted_scores.sort_values(ascending=False).items():
            sov_percentage = (weight / total_weight) * 100
            mention_count = len(df[df['brand'] == brand])
            avg_rank = df[df['brand'] == brand]['rank'].mean()
            
            sov_data.append({
                'brand': brand,
                'mention_count': mention_count,
                'weighted_score': round(weight, 4),
                'sov_percentage': round(sov_percentage, 2),
                'avg_rank': round(avg_rank, 2),
                'rank': len(sov_data) + 1
            })
        
        return {
            'method': 'weighted',
            'total_weight': round(total_weight, 4),
            'unique_brands': len(brand_weighted_scores),
            'sov_data': sov_data
        }
    
    def _calculate_engagement_sov(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算互动量加权SOV"""
        # 计算总互动量
        df['total_engagement'] = (
            df['liked_count'] + 
            df['collected_count'] + 
            df['comment_count'] + 
            df['share_count']
        )
        
        brand_engagement = df.groupby('brand').agg({
            'total_engagement': 'sum',
            'brand': 'count',  # 笔记数量
            'rank': 'mean'     # 平均排名
        }).rename(columns={'brand': 'mention_count'})
        
        total_engagement = df['total_engagement'].sum()
        
        sov_data = []
        for brand, row in brand_engagement.sort_values('total_engagement', ascending=False).iterrows():
            sov_percentage = (row['total_engagement'] / total_engagement) * 100 if total_engagement > 0 else 0
            
            sov_data.append({
                'brand': brand,
                'mention_count': int(row['mention_count']),
                'total_engagement': int(row['total_engagement']),
                'avg_engagement_per_note': round(row['total_engagement'] / row['mention_count'], 2),
                'sov_percentage': round(sov_percentage, 2),
                'avg_rank': round(row['rank'], 2),
                'rank': len(sov_data) + 1
            })
        
        return {
            'method': 'engagement',
            'total_engagement': int(total_engagement),
            'unique_brands': len(brand_engagement),
            'sov_data': sov_data
        }
    
    def _save_sov_results(self, results: Dict[str, Any], keyword: str, method: str, data_dir: str) -> str:
        """保存SOV计算结果"""
        # 确保输出目录存在
        os.makedirs(data_dir, exist_ok=True)
        
        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"sov_results_{keyword}_{method}_{timestamp}.json"
        filepath = os.path.join(data_dir, filename)
        
        # 添加元数据
        results['metadata'] = {
            'keyword': keyword,
            'method': method,
            'calculated_at': datetime.now().isoformat(),
            'tool_version': '1.0'
        }
        
        # 保存JSON文件
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        # 同时保存CSV格式的SOV数据
        csv_filename = f"sov_results_{keyword}_{method}_{timestamp}.csv"
        csv_filepath = os.path.join(data_dir, csv_filename)
        
        sov_df = pd.DataFrame(results['sov_data'])
        sov_df.to_csv(csv_filepath, index=False, encoding='utf-8-sig')
        
        logger.info(f"[SOVCalculatorTool] SOV结果已保存: {filepath}")
        logger.info(f"[SOVCalculatorTool] SOV结果CSV: {csv_filepath}")
        
        return filepath
    
    def _generate_sov_report(self, results: Dict[str, Any], keyword: str, method: str, total_records: int) -> str:
        """生成SOV报告"""
        sov_data = results['sov_data']
        
        if not sov_data:
            return "没有找到品牌SOV数据"
        
        report_lines = []
        report_lines.append(f"关键词: {keyword}")
        report_lines.append(f"计算方法: {method}")
        report_lines.append(f"总记录数: {total_records}")
        report_lines.append(f"涉及品牌数: {results['unique_brands']}")
        report_lines.append("")
        report_lines.append("🏆 Top 10 品牌SOV排名:")
        report_lines.append("-" * 60)
        
        # 显示前10名
        top_brands = sov_data[:10]
        
        if method == 'simple':
            for item in top_brands:
                report_lines.append(
                    f"{item['rank']:2d}. {item['brand']:<20} | "
                    f"笔记数: {item['mention_count']:3d} | "
                    f"SOV: {item['sov_percentage']:6.2f}%"
                )
        elif method == 'weighted':
            for item in top_brands:
                report_lines.append(
                    f"{item['rank']:2d}. {item['brand']:<20} | "
                    f"笔记数: {item['mention_count']:3d} | "
                    f"平均排名: {item['avg_rank']:5.2f} | "
                    f"SOV: {item['sov_percentage']:6.2f}%"
                )
        elif method == 'engagement':
            for item in top_brands:
                report_lines.append(
                    f"{item['rank']:2d}. {item['brand']:<20} | "
                    f"笔记数: {item['mention_count']:3d} | "
                    f"总互动: {item['total_engagement']:6d} | "
                    f"SOV: {item['sov_percentage']:6.2f}%"
                )
        
        # 添加市场集中度分析
        report_lines.append("")
        report_lines.append("📈 市场集中度分析:")
        
        top3_sov = sum(item['sov_percentage'] for item in sov_data[:3])
        top5_sov = sum(item['sov_percentage'] for item in sov_data[:5])
        
        report_lines.append(f"- Top 3 品牌SOV总和: {top3_sov:.2f}%")
        report_lines.append(f"- Top 5 品牌SOV总和: {top5_sov:.2f}%")
        
        # 市场集中度判断
        if top3_sov > 60:
            concentration = "高度集中"
        elif top3_sov > 40:
            concentration = "中度集中"
        else:
            concentration = "分散竞争"
        
        report_lines.append(f"- 市场集中度: {concentration}")
        
        return "\n".join(report_lines) 