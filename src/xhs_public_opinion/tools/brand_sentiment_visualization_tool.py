import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from crewai.tools import BaseTool
import logging
from datetime import datetime
import matplotlib.patches as mpatches
import warnings
import matplotlib.font_manager as fm

from ..store import SupabaseDatabase, FileManager

# 设置中文字体和样式
def setup_chinese_fonts():
    """设置中文字体"""
    chinese_fonts = [
        'SimHei',           # Windows 黑体
        'Microsoft YaHei',  # Windows 微软雅黑
        'PingFang SC',      # macOS 苹方
        'Hiragino Sans GB', # macOS 冬青黑体
        'STHeiti',          # macOS 华文黑体
        'WenQuanYi Micro Hei', # Linux 文泉驿微米黑
        'Noto Sans CJK SC', # Google Noto
        'Source Han Sans CN', # 思源黑体
        'Arial Unicode MS'   # 通用 Unicode 字体
    ]
    
    available_fonts = [f.name for f in fm.fontManager.ttflist]
    
    for font in chinese_fonts:
        if font in available_fonts:
            print(f"使用中文字体: {font}")
            return font
    
    print("警告: 未找到合适的中文字体，可能无法正确显示中文")
    return 'DejaVu Sans'

# 设置字体
chinese_font = setup_chinese_fonts()
plt.rcParams['font.sans-serif'] = [chinese_font, 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['font.size'] = 10

# 设置样式
try:
    plt.style.use('seaborn-v0_8-whitegrid')
except:
    plt.style.use('default')
    print("使用默认样式")

warnings.filterwarnings('ignore')

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BrandSentimentVisualizationTool:
    """品牌情感分析可视化工具 - 生成目标品牌情感倾向舆情图表"""
    name: str = "brand_sentiment_visualization"
    description: str = "基于数据库情感数据生成目标品牌在不同档位的情感倾向舆情可视化图表"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 初始化数据库连接和文件管理器
        self.db = SupabaseDatabase()
        self.file_manager = FileManager()
    
    def _run(self, keyword: str, target_brand: str, output_dir: str = "outputs") -> str:
        """
        生成品牌情感分析可视化图表
        
        Args:
            keyword: 关键词
            target_brand: 目标品牌
            output_dir: 输出目录
        
        Returns:
            生成的图表文件路径
        """
        try:
            if not self.db.client:
                return "❌ 数据库连接失败，请检查环境变量配置"
            
            if not target_brand:
                return "❌ 请指定目标品牌"
            
            logger.info(f"[BrandSentimentVisualization] 开始生成关键词 '{keyword}' 目标品牌 '{target_brand}' 的情感分析图表...")
            
            # 加载情感数据
            current_sentiment_data, previous_sentiment_data = self._load_sentiment_data_from_db(keyword, target_brand)
            
            if not current_sentiment_data:
                return f"❌ 未找到关键词 '{keyword}' 目标品牌 '{target_brand}' 的情感数据"
            
            # 生成情感分析图表
            chart_path = self._generate_sentiment_chart(keyword, target_brand, current_sentiment_data, previous_sentiment_data, output_dir)
            
            return f"✅ 品牌情感分析图表生成完成: {chart_path}"
            
        except Exception as e:
            logger.error(f"[BrandSentimentVisualization] 图表生成失败: {e}")
            return f"❌ 图表生成失败: {str(e)}"
    
    def _load_sentiment_data_from_db(self, keyword: str, target_brand: str) -> Tuple[Optional[Dict], Optional[Dict]]:
        """从数据库加载情感数据"""
        try:
            current_data = {}
            previous_data = {}
            
            # 查询所有档位的情感数据
            tiers = [20, 50, 100]  # TOP20, TOP50, TOP100
            
            for tier in tiers:
                # 使用统一的数据库方法获取数据
                raw_data = self.db.get_sentiment_visualization_data(keyword, target_brand, max_rank=tier, limit=200)
                
                if raw_data:
                    df = pd.DataFrame(raw_data)
                    df['created_at'] = pd.to_datetime(df['created_at'])
                    df['date'] = df['created_at'].dt.date
                    
                    # 按日期分组
                    unique_dates = sorted(df['date'].unique(), reverse=True)
                    logger.info(f"tier {tier}, unique_dates: {unique_dates}")
                    
                    if len(unique_dates) >= 1:
                        # 最新日期的数据
                        current_date = unique_dates[0]
                        current_df = df[df['date'] == current_date]
                        
                        # 统计情感分布
                        sentiment_counts = current_df['brand_emotion'].value_counts()
                        current_data[f"TOP{tier}"] = {
                            'positive': sentiment_counts.get('正向', 0),
                            'negative': sentiment_counts.get('负向', 0),
                            'neutral': sentiment_counts.get('中立', 0),
                            'total': len(current_df),
                            'date': current_date
                        }
                        logger.info(f"current data TOP{tier}: {current_data[f'TOP{tier}']}")
                    
                    if len(unique_dates) >= 2:
                        # 上一期数据
                        previous_date = unique_dates[1]
                        previous_df = df[df['date'] == previous_date]
                        
                        # 统计情感分布
                        sentiment_counts = previous_df['brand_emotion'].value_counts()
                        previous_data[f"TOP{tier}"] = {
                            'positive': sentiment_counts.get('正向', 0),
                            'negative': sentiment_counts.get('负向', 0),
                            'neutral': sentiment_counts.get('中立', 0),
                            'total': len(previous_df),
                            'date': previous_date
                        }
                        logger.info(f"previous data TOP{tier}: {previous_data[f'TOP{tier}']}")
            
            return current_data, previous_data if previous_data else None
            
        except Exception as e:
            logger.error(f"从数据库加载情感数据失败: {e}")
            return None, None
    
    def _generate_sentiment_chart(self, keyword: str, target_brand: str, 
                                current_data: Dict, previous_data: Optional[Dict], 
                                output_dir: str) -> str:
        """生成情感分析图表"""
        
        # 创建输出目录
        chart_dir = self.file_manager.build_path(output_dir, keyword, "charts")
        self.file_manager.ensure_directory(chart_dir)
        
        # 创建图表 - 1行3列布局，增加高度
        fig, axes = plt.subplots(1, 3, figsize=(24, 12))
        
        # 调整子图间距，为标题和标注留出更多空间
        plt.subplots_adjust(top=0.85, bottom=0.12, left=0.06, right=0.94, wspace=0.25)
        
        # 设置总标题，降低位置
        fig.suptitle(f'【{keyword}】{target_brand} 品牌情感倾向分析', 
                    fontsize=18, fontweight='bold', y=0.95,
                    fontproperties=fm.FontProperties(family=chinese_font))
        
        # 情感颜色映射
        sentiment_colors = {
            'positive': '#4CAF50',  # 绿色
            'negative': '#F44336',  # 红色
            'neutral': '#FFC107'    # 黄色
        }
        
        sentiment_labels = {
            'positive': '正向',
            'negative': '负向', 
            'neutral': '中立'
        }
        
        tiers = ['TOP20', 'TOP50', 'TOP100']
        
        # 绘制三个档位的柱状图
        for i, tier in enumerate(tiers):
            ax = axes[i]
            
            if tier in current_data and current_data[tier]['total'] > 0:
                data = current_data[tier]
                
                # 准备数据
                categories = ['正向', '负向', '中立']
                current_values = [data['positive'], data['negative'], data['neutral']]
                colors = [sentiment_colors['positive'], sentiment_colors['negative'], sentiment_colors['neutral']]
                
                # 计算环比变化
                changes = [None, None, None]
                if previous_data and tier in previous_data:
                    prev_data = previous_data[tier]
                    changes = [
                        data['positive'] - prev_data['positive'],
                        data['negative'] - prev_data['negative'], 
                        data['neutral'] - prev_data['neutral']
                    ]
                
                # 绘制柱状图
                bars = ax.bar(categories, current_values, color=colors, alpha=0.8, 
                             edgecolor='black', linewidth=1)
                
                # 添加数值标签和环比变化
                max_value = max(current_values) if current_values else 1
                for j, (bar, value, change) in enumerate(zip(bars, current_values, changes)):
                    height = bar.get_height()
                    
                    # 当前数值标签
                    ax.text(bar.get_x() + bar.get_width()/2., height + max_value * 0.01,
                           f'{value}', ha='center', va='bottom', fontsize=12, fontweight='bold',
                           fontproperties=fm.FontProperties(family=chinese_font))
                    
                    # 环比变化标签
                    if change is not None:
                        if change > 0:
                            change_text = f'↑+{change}'
                            change_color = '#FF4444'
                        elif change < 0:
                            change_text = f'↓{change}'
                            change_color = '#44AA44'
                        else:
                            change_text = f'→{change}'
                            change_color = '#888888'
                        
                        ax.text(bar.get_x() + bar.get_width()/2., height + max_value * 0.1,
                               change_text, ha='center', va='bottom', fontsize=10,
                               color=change_color, fontweight='bold',
                               fontproperties=fm.FontProperties(family=chinese_font))
                
                # 设置图表样式
                ax.set_ylabel('笔记数量', fontsize=12,
                             fontproperties=fm.FontProperties(family=chinese_font))
                
                # 分两行显示标题信息，减少拥挤
                ax.set_title(f'{tier} 情感分布\n总笔记: {data["total"]} ', 
                           fontsize=13, fontweight='bold', pad=15,
                           fontproperties=fm.FontProperties(family=chinese_font))
                
                # 设置x轴标签字体
                ax.set_xticklabels(categories, fontsize=11, 
                                 fontproperties=fm.FontProperties(family=chinese_font))
                
                # 美化样式
                ax.spines['top'].set_visible(False)
                ax.spines['right'].set_visible(False)
                ax.grid(axis='y', alpha=0.3, linestyle='--')
                
                # 设置y轴范围，为标签留出更多空间
                ax.set_ylim(0, max_value * 1.25)
                
                # 添加环比说明，调整位置
                if previous_data and tier in previous_data:
                    ax.text(0.02, 0.95, '环比: ↑上升 ↓下降 →持平', 
                           transform=ax.transAxes, fontsize=9, va='top', 
                           bbox=dict(boxstyle="round,pad=0.3", facecolor='lightblue', alpha=0.7),
                           fontproperties=fm.FontProperties(family=chinese_font))
                
            else:
                ax.text(0.5, 0.5, '暂无数据', ha='center', va='center',
                       transform=ax.transAxes, fontsize=16,
                       fontproperties=fm.FontProperties(family=chinese_font))
                ax.set_title(f'{tier} 情感分布', fontsize=13, fontweight='bold', pad=15,
                           fontproperties=fm.FontProperties(family=chinese_font))
        
        # 添加图例和说明
        self._add_sentiment_annotations(fig, keyword, target_brand, current_data, previous_data)
        
        # 生成文件名并保存
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        chart_filename = f"BrandSentiment_{target_brand}_{keyword}_{timestamp}.png"
        chart_path = self.file_manager.build_path(chart_dir, chart_filename)
        
        plt.savefig(chart_path, dpi=300, bbox_inches='tight', facecolor='white', edgecolor='none')
        plt.close()
        
        logger.info(f"品牌情感分析图表已保存: {chart_path}")
        return chart_path
    
    def _add_sentiment_annotations(self, fig, keyword: str, target_brand: str, 
                                 current_data: Dict, previous_data: Optional[Dict]):
        """添加情感分析图表的说明信息"""
        
        # 统计总体情况
        total_notes = sum(data['total'] for data in current_data.values() if 'total' in data)
        total_positive = sum(data['positive'] for data in current_data.values() if 'positive' in data)
        total_negative = sum(data['negative'] for data in current_data.values() if 'negative' in data)
        total_neutral = sum(data['neutral'] for data in current_data.values() if 'neutral' in data)
        
        # 计算总体情感比例
        if total_notes > 0:
            positive_ratio = (total_positive / total_notes) * 100
            negative_ratio = (total_negative / total_notes) * 100
            neutral_ratio = (total_neutral / total_notes) * 100
        else:
            positive_ratio = negative_ratio = neutral_ratio = 0
        
        # 环比总变化
        if previous_data:
            prev_total_notes = sum(data['total'] for data in previous_data.values() if 'total' in data)
            notes_change = total_notes - prev_total_notes
            change_text = f"总笔记数环比变化: {notes_change:+d}"
        else:
            change_text = "暂无环比数据"
        
        # 添加说明文字
        summary_text = (f"品牌 {target_brand} 在关键词 \"{keyword}\" 下的情感分布:\n"
                       f"总笔记数: {total_notes} | "
                       f"正向: {positive_ratio:.1f}% | 负向: {negative_ratio:.1f}% | 中立: {neutral_ratio:.1f}%\n"
                       f"{change_text}")
        
        fig.text(0.5, 0.04, summary_text, ha='center', va='bottom', 
                fontsize=11, style='italic', wrap=True,
                fontproperties=fm.FontProperties(family=chinese_font),
                bbox=dict(boxstyle="round,pad=0.5", facecolor='lightblue', alpha=0.6))
        
        # 版权信息
        copyright_text = f"Copyright © 2025 乐见真言科技（苏州）有限公司"
        fig.text(0.5, 0.01, copyright_text, ha='center', va='bottom', 
                fontsize=9, color='gray',
                fontproperties=fm.FontProperties(family=chinese_font)) 