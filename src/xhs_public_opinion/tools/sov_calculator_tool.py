import os
import json
import glob
import pandas as pd
from typing import Dict, Any, Optional, List
from crewai.tools import BaseTool
import logging
from datetime import datetime

# 导入品牌标准化工具
from .brand_normalizer import get_brand_normalizer
from ..store import SupabaseDatabase, FileManager

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SOVCalculatorTool:
    """SOV计算工具 - 计算各品牌在关键词下的声量占比（Share of Voice）"""
    name: str = "sov_calculator"
    description: str = "基于合并后的CSV数据，计算各品牌在指定关键词下的SOV（声量占比），支持分档位计算并写入数据库"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # 初始化品牌标准化器
        self.brand_normalizer = get_brand_normalizer()
        
        # 初始化数据库连接和文件管理器
        self.db = SupabaseDatabase()
        self.file_manager = FileManager()
    
    def _run(self, keyword: str, input_data_dir: str = "data/export", output_data_dir: str = "outputs", method: str = "weighted") -> str:
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
            csv_file = self._find_csv_file(keyword, input_data_dir)
            if not csv_file:
                return f"未找到关键词 '{keyword}' 的数据文件，请先使用DataMergerTool生成宽表数据"
            
            # 2. 读取CSV数据
            df = pd.read_csv(csv_file)
            logger.info(f"[SOVCalculatorTool] 读取数据文件: {csv_file}, 记录数: {len(df)}")
            
            # 3. 数据预处理
            processed_df = self._preprocess_data(df)
            
            # 4. 分档位计算SOV
            tier_results = self._calculate_tiered_sov(processed_df, method)
            
            # 5. 保存结果到CSV
            result_files = self._save_tiered_sov_results(tier_results, keyword, method, output_data_dir)
            
            # 6. 写入数据库
            db_write_result = self._write_sov_to_database(tier_results, keyword, method)
            #db_write_result= '123'
            # 7. 生成报告
            report = self._generate_tiered_sov_report(tier_results, keyword, method, len(processed_df))
            
            return f"""✅ SOV计算完成！

📊 SOV分析报告:
{report}

📁 结果文件: {result_files}
📁 数据源: {csv_file}
📁 数据库写入: {db_write_result}"""
            
        except Exception as e:
            logger.error(f"[SOVCalculatorTool] SOV计算失败: {e}")
            return f"SOV计算失败: {str(e)}"
    
    def _find_csv_file(self, keyword: str, data_dir: str) -> Optional[str]:
        """查找指定关键词的最新CSV文件"""
        pattern = f"{data_dir}/{keyword}/merged_data_*.csv"
        files = self.file_manager.find_files_by_pattern(pattern)
        
        if not files:
            return None
        
        # 返回最新的文件
        latest_file = self.file_manager.find_latest_file(files)
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
                    brands = self.file_manager.parse_json_string(brand_list_str)
                    
                    # 处理双重编码的情况
                    if isinstance(brands, str):
                        try:
                            brands = self.file_manager.parse_json_string(brands)
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

                #logger.info(f"[SOVCalculatorTool] rank: {row['rank']}，brands:  {brands}")
        
                # 为每个品牌创建一条记录
                for brand in brands:
                    if brand and brand.strip():
                        # 标准化品牌名
                        brand_normalizer = self.brand_normalizer
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
        #ogger.info(f"[SOVCalculatorTool] 展开后的品牌记录: {expanded_df.head(10)}")

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
    
    def _calculate_tiered_sov(self, df: pd.DataFrame, method: str) -> Dict[str, Any]:
        """分档位计算SOV（top20、top50、top100）"""
        tiers = {
            'top20': 20,
            'top50': 50, 
            'top100': 100
        }
        
        tier_results = {}
        
        for tier_name, tier_limit in tiers.items():
            logger.info(f"[SOVCalculatorTool] 计算 {tier_name} SOV...")
            
            # 筛选对应档位的数据
            tier_df = df[df['rank'] <= tier_limit].copy()
            
            if tier_df.empty:
                logger.warning(f"[SOVCalculatorTool] {tier_name} 档位没有数据")
                tier_results[tier_name] = {
                    'method': method,
                    'tier': tier_name,
                    'tier_limit': tier_limit,
                    'total_records': 0,
                    'unique_brands': 0,
                    'sov_data': []
                }
                continue
            
            # 根据方法计算SOV
            if method == "simple":
                sov_result = self._calculate_simple_sov(tier_df)
            elif method == "weighted":
                sov_result = self._calculate_weighted_sov(tier_df)
            elif method == "engagement":
                sov_result = self._calculate_engagement_sov(tier_df)
            else:
                raise ValueError(f"不支持的计算方法: {method}")
            
            # 添加档位信息
            sov_result['tier'] = tier_name
            sov_result['tier_limit'] = tier_limit
            sov_result['total_records'] = len(tier_df)
            
            tier_results[tier_name] = sov_result
            
            logger.info(f"[SOVCalculatorTool] {tier_name} SOV计算完成，记录数: {len(tier_df)}, 品牌数: {sov_result['unique_brands']}")
        
        return tier_results
    
    def _save_tiered_sov_results(self, tier_results: Dict[str, Any], keyword: str, method: str, data_dir: str) -> str:
        """保存分档位SOV计算结果到CSV"""
        # 确保输出目录存在
        output_dir = self.file_manager.build_path(data_dir, keyword)
        self.file_manager.ensure_directory(output_dir)
        
        # 生成时间戳
        timestamp = datetime.now().strftime("%Y%m%d")
        
        result_files = []
        
        # 合并所有档位的数据到一个CSV文件
        all_sov_data = []
        
        for tier_name, tier_data in tier_results.items():
            if not tier_data.get('sov_data'):
                logger.warning(f"[SOVCalculatorTool] {tier_name} 档位没有SOV数据，跳过")
                continue
            
            # 为每条记录添加档位信息
            for sov_record in tier_data['sov_data']:
                record = sov_record.copy()
                record['keyword'] = keyword
                record['method'] = method
                record['tier'] = tier_name
                record['tier_limit'] = tier_data['tier_limit']
                record['total_records'] = tier_data['total_records']
                record['calculated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                all_sov_data.append(record)
        
        if all_sov_data:
            # CSV文件名
            csv_filename = f"SOV_all_tiers_{method}_{timestamp}.csv"
            csv_filepath = self.file_manager.build_path(output_dir, csv_filename)
            
            # 创建DataFrame
            sov_df = pd.DataFrame(all_sov_data)
            
            # 重新排列列顺序
            columns_order = ['keyword', 'method', 'tier', 'tier_limit', 'rank', 'brand', 'mention_count', 'sov_percentage']
            
            # 根据计算方法添加特定列
            if method == 'weighted':
                columns_order.extend(['weighted_score', 'avg_rank'])
            elif method == 'engagement':
                columns_order.extend(['total_engagement', 'avg_engagement_per_note', 'avg_rank'])
            
            columns_order.extend(['total_records', 'calculated_at'])
            
            # 筛选存在的列
            available_columns = [col for col in columns_order if col in sov_df.columns]
            sov_df = sov_df[available_columns]
            
            # 保存CSV
            self.file_manager.save_csv(sov_df, csv_filepath)
            result_files.append(csv_filepath)
            
            logger.info(f"[SOVCalculatorTool] 所有档位SOV结果已保存: {csv_filepath}")
        
        # 同时保存合并的JSON文件
        json_filename = f"SOV_all_tiers_{method}_{timestamp}.json"
        json_filepath = self.file_manager.build_path(output_dir, json_filename)
        
        # 添加元数据
        tier_results['metadata'] = {
            'keyword': keyword,
            'method': method,
            'calculated_at': datetime.now().isoformat(),
            'tool_version': '2.0'
        }
        
        self.file_manager.save_json(tier_results, json_filepath)
        result_files.append(json_filepath)
        logger.info(f"[SOVCalculatorTool] 合并JSON结果已保存: {json_filepath}")
        
        return ", ".join(result_files)
    
    def _generate_tiered_sov_report(self, tier_results: Dict[str, Any], keyword: str, method: str, total_records: int) -> str:
        """生成分档位SOV报告"""
        if not tier_results:
            return "没有找到SOV数据"
        
        report_lines = []
        report_lines.append(f"关键词: {keyword}")
        report_lines.append(f"计算方法: {method}")
        report_lines.append(f"总记录数: {total_records}")
        report_lines.append("")
        
        # 为每个档位生成报告
        for tier_name in ['top20', 'top50', 'top100']:
            if tier_name not in tier_results:
                continue
                
            tier_data = tier_results[tier_name]
            sov_data = tier_data.get('sov_data', [])
            
            if not sov_data:
                report_lines.append(f"🏆 {tier_name.upper()} 档位: 无数据")
                report_lines.append("")
                continue
            
            report_lines.append(f"🏆 {tier_name.upper()} 档位 (前{tier_data['tier_limit']}名):")
            report_lines.append(f"   记录数: {tier_data['total_records']}, 涉及品牌数: {tier_data['unique_brands']}")
            report_lines.append("-" * 60)
            
            # 显示前10名品牌
            top_brands = sov_data[:10]
            
            if method == 'simple':
                for item in top_brands:
                    report_lines.append(
                        f"   {item['rank']:2d}. {item['brand']:<20} | "
                        f"笔记数: {item['mention_count']:3d} | "
                        f"SOV: {item['sov_percentage']:6.2f}%"
                    )
            elif method == 'weighted':
                for item in top_brands:
                    report_lines.append(
                        f"   {item['rank']:2d}. {item['brand']:<20} | "
                        f"笔记数: {item['mention_count']:3d} | "
                        f"平均排名: {item['avg_rank']:5.2f} | "
                        f"SOV: {item['sov_percentage']:6.2f}%"
                    )
            elif method == 'engagement':
                for item in top_brands:
                    report_lines.append(
                        f"   {item['rank']:2d}. {item['brand']:<20} | "
                        f"笔记数: {item['mention_count']:3d} | "
                        f"总互动: {item['total_engagement']:6d} | "
                        f"SOV: {item['sov_percentage']:6.2f}%"
                    )
            
            # 添加市场集中度分析
            if len(sov_data) >= 3:
                top3_sov = sum(item['sov_percentage'] for item in sov_data[:3])
                top5_sov = sum(item['sov_percentage'] for item in sov_data[:5]) if len(sov_data) >= 5 else top3_sov
                
                report_lines.append("")
                report_lines.append(f"   📈 {tier_name.upper()} 市场集中度:")
                report_lines.append(f"   - Top 3 品牌SOV总和: {top3_sov:.2f}%")
                if len(sov_data) >= 5:
                    report_lines.append(f"   - Top 5 品牌SOV总和: {top5_sov:.2f}%")
                
                # 市场集中度判断
                if top3_sov > 60:
                    concentration = "高度集中"
                elif top3_sov > 40:
                    concentration = "中度集中"
                else:
                    concentration = "分散竞争"
                
                report_lines.append(f"   - 市场集中度: {concentration}")
            
            report_lines.append("")
        
        # 添加档位对比分析
        if len(tier_results) > 1:
            report_lines.append("📊 档位对比分析:")
            report_lines.append("-" * 60)
            
            # 找出在所有档位都出现的品牌
            all_brands = set()
            for tier_data in tier_results.values():
                if isinstance(tier_data, dict) and 'sov_data' in tier_data:
                    for item in tier_data['sov_data']:
                        all_brands.add(item['brand'])
            
            # 显示主要品牌在不同档位的表现
            main_brands = list(all_brands)[:5]  # 取前5个品牌进行对比
            
            for brand in main_brands:
                brand_performance = []
                for tier_name in ['top20', 'top50', 'top100']:
                    if tier_name in tier_results:
                        tier_data = tier_results[tier_name]
                        for item in tier_data.get('sov_data', []):
                            if item['brand'] == brand:
                                brand_performance.append(f"{tier_name}: {item['sov_percentage']:.2f}%")
                                break
                
                if brand_performance:
                    report_lines.append(f"   {brand}: {' | '.join(brand_performance)}")
        
        return "\n".join(report_lines)
    
    def _write_sov_to_database(self, tier_results: Dict[str, Any], keyword: str, method: str) -> str:
        """将SOV结果写入Supabase数据库表xhs_keyword_sov_result"""
        if not self.db.is_connected():
            return "❌ 数据库连接未初始化，跳过数据库写入"
        
        try:
            logger.info(f"[SOVCalculatorTool] 开始写入SOV结果到数据库...")
            
            # 准备要插入的数据
            data_to_insert = []
            
            for tier_name, tier_data in tier_results.items():
                if tier_name == 'metadata':  # 跳过元数据
                    continue
                    
                if not tier_data.get('sov_data'):
                    logger.warning(f"[SOVCalculatorTool] {tier_name} 档位没有SOV数据，跳过写入")
                    continue
                
                for sov_item in tier_data['sov_data']:
                    record = {
                        "keyword": self.db.safe_str(keyword),
                        "tier_limit": self.db.safe_int(tier_data.get('tier_limit')),
                        "brand": self.db.safe_str(sov_item.get('brand')),
                        "rank": self.db.safe_int(sov_item.get('rank')),
                        "sov_percentage": self.db.safe_float(sov_item.get('sov_percentage')),
                        "mention_count": self.db.safe_int(sov_item.get('mention_count')),
                        "total_records": self.db.safe_int(tier_data.get('total_records')),
                        "unique_brands": self.db.safe_int(tier_data.get('unique_brands')),
                    }
                    
                    # 根据计算方法添加特定字段
                    if method == 'weighted':
                        record["weighted_score"] = self.db.safe_float(sov_item.get('weighted_score'))
                        record["avg_rank"] = self.db.safe_float(sov_item.get('avg_rank'))
                    elif method == 'engagement':
                        record["total_engagement"] = self.db.safe_int(sov_item.get('total_engagement'))
                        record["avg_engagement_per_note"] = self.db.safe_float(sov_item.get('avg_engagement_per_note'))
                        record["avg_rank"] = self.db.safe_float(sov_item.get('avg_rank'))
                    
                    data_to_insert.append(record)
            
            if not data_to_insert:
                return "❌ 没有SOV数据需要写入数据库"
            
            # 使用统一的数据库接口批量插入
            result = self.db.batch_insert_sov_data(data_to_insert)
            
            if result['success']:
                logger.info(f"[SOVCalculatorTool] ✅ 成功写入 {result['inserted_count']} 条SOV记录到数据库")
                return f"✅ 成功写入 {result['inserted_count']} 条SOV记录到数据库"
            else:
                logger.error(f"[SOVCalculatorTool] ❌ {result['error']}")
                return f"❌ {result['error']}"
            
        except Exception as e:
            error_msg = f"写入数据库失败: {str(e)}"
            logger.error(f"[SOVCalculatorTool] ❌ {error_msg}")
            return f"❌ {error_msg}" 