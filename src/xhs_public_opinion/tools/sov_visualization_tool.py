"""
SOV Visualization Tool

Creates visualizations for Share of Voice (SOV) analysis results.
Uses the store layer's FileManager for all file and image operations.
"""

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
from matplotlib.gridspec import GridSpec
import warnings
import matplotlib.font_manager as fm
from pydantic import BaseModel, Field

from ..store import SupabaseDatabase, FileManager
    
# 设置中文字体和样式 - 改进字体配置
def setup_chinese_fonts():
    """设置中文字体"""
    # 常见的中文字体列表
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
    
    # 获取系统所有字体
    available_fonts = [f.name for f in fm.fontManager.ttflist]
    
    # 找到第一个可用的中文字体
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

# 忽略警告
warnings.filterwarnings('ignore')

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SOVVisualizationTool:
    """SOV可视化工具 - 生成品牌SOV周报图表（仅支持数据库模式）"""
    name: str = "sov_visualization"
    description: str = "基于数据库SOV数据生成品牌周报可视化图表，支持三档位并排显示"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 初始化数据库连接
        self.db = SupabaseDatabase()
        self.file_manager = FileManager()
    
    def _run(self, keyword: str, 
             output_dir: str = "outputs",
             target_brand: str = "") -> str:
        """
        生成SOV可视化图表
        
        Args:
            keyword: 关键词
            output_dir: 输出目录
            target_brand: 目标品牌（用于高亮显示）
        
        Returns:
            生成的图表文件路径
        """
        try:
            if not self.db.client:
                return "❌ 数据库连接失败，请检查环境变量配置"
            
            logger.info(f"[SOVVisualization] 开始生成关键词 '{keyword}' 的三档位图表...")
            
            # 加载所有档位数据
            current_data, previous_data = self._load_sov_data_from_db(keyword)
            if not current_data:
                raise ValueError(f"未找到关键词 '{keyword}' 的数据")
            
            # 生成三档位并排图表
            chart_path = self._generate_three_tier_chart(keyword, current_data, previous_data, output_dir, target_brand)
            
            return f"✅ SOV图表生成完成: {chart_path}"
            
        except Exception as e:
            logger.error(f"[SOVVisualization] 图表生成失败: {e}")
            return f"❌ 图表生成失败: {str(e)}"
    
    def _load_sov_data_from_db(self, keyword: str) -> Tuple[Optional[Dict], Optional[Dict]]:
        """从数据库加载SOV数据"""
        try:
            current_data = {}
            previous_data = {}
            
            # 查询所有档位
            tiers_to_query = ["20","50","100"]
            
            for t in tiers_to_query:
                # 使用统一的数据库方法获取数据
                raw_data = self.db.get_sov_visualization_data(keyword, tier_limit=t, limit=100)
                
                if raw_data:
                    df = pd.DataFrame(raw_data)
                    df['created_at'] = pd.to_datetime(df['created_at'])
                    df['date'] = df['created_at'].dt.date
          
                    unique_dates = sorted(df['date'].unique(), reverse=True)
                    
                    if len(unique_dates) >= 1:
                        # 最新日期的数据
                        current_date = unique_dates[0]
                        current_df = df[df['date'] == current_date]
                        current_df = current_df.sort_values('sov_percentage', ascending=False)
                        current_data[t] = current_df.head(10).to_dict('records')
             
                    
                    if len(unique_dates) >= 2:
                        # 第二新日期的数据
                        previous_date = unique_dates[1]
                        previous_df = df[df['date'] == previous_date]
                        previous_df = previous_df.sort_values('sov_percentage', ascending=False)
                        previous_data[t] = previous_df.head(10).to_dict('records')
                  
            
            # 返回所有档位的数据字典
            return current_data, previous_data if previous_data else None
            
        except Exception as e:
            logger.error(f"从数据库加载SOV数据失败: {e}")
            return None, None
    
    def _generate_three_tier_chart(self, keyword: str, current_data: Dict, 
                                 previous_data: Optional[Dict], output_dir: str, target_brand: str) -> str:
        """生成三档位并排图表"""
        
        # 创建输出目录
        chart_dir = self.file_manager.build_path(output_dir, keyword, "charts")
        self.file_manager.ensure_directory(chart_dir)
        
        # 创建图表 - 1行3列布局，增加高度并调整子图间距
        fig, axes = plt.subplots(1, 3, figsize=(24, 12))
        
        # 调整子图间距，为标题和注释留出空间
        plt.subplots_adjust(top=0.88, bottom=0.15, left=0.05, right=0.95, wspace=0.3)
        
        # 设置图表标题（降低位置避免重叠）
        fig.suptitle(f'【{keyword}】品牌SOV分析', 
                    fontsize=20, fontweight='bold', y=0.93,
                    fontproperties=fm.FontProperties(family=chinese_font))
        
        tiers = ["20", "50", "100"]
        tier_names = ["TOP20", "TOP50", "TOP100"]
        
        for i, (tier, tier_name) in enumerate(zip(tiers, tier_names)):
            ax = axes[i]
            
            if tier in current_data and current_data[tier]:
                # 准备当前档位的数据
                current_tier_data = current_data[tier]
                previous_tier_data = previous_data.get(tier, []) if previous_data else []
                
                # 绘制该档位的SOV图表
                self._draw_tier_sov_chart(ax, current_tier_data, previous_tier_data, tier_name, target_brand)
            else:
                # 无数据时显示提示
                ax.text(0.5, 0.5, f'暂无{tier_name}数据', ha='center', va='center', 
                       fontsize=16, transform=ax.transAxes,
                       fontproperties=fm.FontProperties(family=chinese_font))
                ax.set_title(tier_name, fontsize=16, fontweight='bold',
                           fontproperties=fm.FontProperties(family=chinese_font))
        
        # 添加整体说明
        self._add_three_tier_annotations(fig, keyword, current_data, previous_data)
        
        # 保存图表
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        chart_filename = f"SOV_ThreeTier_{keyword}_{timestamp}.png"
        chart_path = self.file_manager.build_path(chart_dir, chart_filename)
        
        # 不使用tight_layout，因为我们已经手动调整了布局
        plt.savefig(chart_path, dpi=300, bbox_inches='tight', facecolor='white', edgecolor='none')
        plt.close()
        
        logger.info(f"图表已保存: {chart_path}")
        return chart_path
    
    def _draw_tier_sov_chart(self, ax, current_data: List[Dict], 
                           previous_data: List[Dict], tier_name: str, target_brand: str):
        """绘制单个档位的SOV图表，包含环比变化"""
        
        if not current_data:
            ax.text(0.5, 0.5, '暂无数据', ha='center', va='center', 
                   fontsize=16, transform=ax.transAxes,
                   fontproperties=fm.FontProperties(family=chinese_font))
            return
        
        # 提取品牌和SOV数据
        brands = [item['brand'] for item in current_data]
        sov_values = [float(item['sov_percentage']) for item in current_data]
        logger.info(f"[_draw_tier_sov_chart] current data tier : {tier_name}, brands: {brands}, sov_values: {sov_values}")
        
        # 定义y轴位置
        y_pos = np.arange(len(brands))
        
        # 计算环比变化
        previous_dict = {item['brand']: float(item['sov_percentage']) 
                        for item in previous_data} if previous_data else {}
        
        changes = []
        trends = []
        
        for brand, current_sov in zip(brands, sov_values):
            if brand in previous_dict:
                previous_sov = previous_dict[brand]
                change = current_sov - previous_sov
                changes.append(change)
                
                # 确定趋势箭头
                if change > 0.1:
                    trends.append('↑')
                elif change < -0.1:
                    trends.append('↓')
                else:
                    trends.append('→')
            else:
                changes.append(None)  # 新品牌
                trends.append('🆕')
        
  
        # 生成颜色 - 使用渐变色而非硬编码
        colors = plt.cm.Set3(np.linspace(0, 1, len(brands)))
        
        # 绘制横向条形图

        
        # 绘制横向条形图，目标品牌使用特殊样式
        bars = []
        for i, (brand, sov, color) in enumerate(zip(brands, sov_values, colors)):
            if target_brand and brand.lower() == target_brand.lower():
                # 目标品牌特殊样式：更粗的边框
                bar = ax.barh(i, sov, color=color, height=0.7, alpha=0.9, 
                             edgecolor='black', linewidth=2)
            else:
                bar = ax.barh(i, sov, color=color, height=0.7, alpha=0.8)
            bars.extend(bar)
        
        # 设置品牌标签（排名 + 品牌名）- 明确指定字体，目标品牌加粗
        ax.set_yticks(y_pos)
        brand_labels = []
        for i, brand in enumerate(brands):
            brand_labels.append(f"{i+1}. {brand}")
        
        ax.set_yticklabels(brand_labels, fontsize=10, 
                          fontproperties=fm.FontProperties(family=chinese_font))
        
        # 为目标品牌的标签单独设置加粗样式
        if target_brand:
            for i, (brand, tick) in enumerate(zip(brands, ax.get_yticklabels())):
                if brand.lower() == target_brand.lower():
                    tick.set_fontweight('bold')
                    tick.set_fontsize(11)  # 稍大字体
        
        # 添加SOV数值标签
        max_sov = max(sov_values) if sov_values else 1
        for i, (bar, sov, change, trend) in enumerate(zip(bars, sov_values, changes, trends)):
            width = bar.get_width()
            
            # SOV百分比标签
            ax.text(width + max_sov * 0.01, bar.get_y() + bar.get_height()/2, 
                   f'{sov:.1f}%', ha='left', va='center', fontsize=9, fontweight='bold',
                   fontproperties=fm.FontProperties(family=chinese_font))
            
            # 环比变化标签
            if change is not None:
                # 趋势箭头
                trend_color = '#FF4444' if trend == '↑' else '#44AA44' if trend == '↓' else '#888888'
                ax.text(width + max_sov * 0.15, bar.get_y() + bar.get_height()/2, 
                       trend, ha='center', va='center', fontsize=12, 
                       color=trend_color, fontweight='bold')
                
                # 变化数值
                if trend != '→':
                    change_text = f'+{change:.1f}' if change > 0 else f'{change:.1f}'
                    ax.text(width + max_sov * 0.22, bar.get_y() + bar.get_height()/2, 
                           change_text, ha='left', va='center', fontsize=8, 
                           color=trend_color, style='italic')
            else:
                # 新品牌标识
                ax.text(width + max_sov * 0.15, bar.get_y() + bar.get_height()/2, 
                       '🆕', ha='center', va='center', fontsize=10)
                ax.text(width + max_sov * 0.22, bar.get_y() + bar.get_height()/2, 
                       'NEW', ha='left', va='center', fontsize=8, 
                       color='#FF8800', fontweight='bold')
        
        # 设置图表样式 - 明确指定字体
        ax.set_xlabel('SOV (%)', fontsize=11, 
                     fontproperties=fm.FontProperties(family=chinese_font))
        ax.set_title(f'{tier_name} SOV排名', fontsize=14, fontweight='bold', pad=10,
                    fontproperties=fm.FontProperties(family=chinese_font))
        ax.set_xlim(0, max_sov * 1.35)  # 减少右边距，为环比数据留空间
        ax.invert_yaxis()
        
        # 美化样式
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.grid(axis='x', alpha=0.3, linestyle='--')
        
        # 添加说明文字 - 调整位置到图表外部
        if previous_data:
            # 将说明文字放到图表顶部外侧，避免与图表重叠
            ax.text(0.02, 1.02, '环比变化: ↑上升 ↓下降 →持平 🆕新上榜', 
                   transform=ax.transAxes, fontsize=8, va='bottom', 
                   bbox=dict(boxstyle="round,pad=0.3", facecolor='lightblue', alpha=0.6),
                   fontproperties=fm.FontProperties(family=chinese_font))
    
    def _add_three_tier_annotations(self, fig, keyword: str, current_data: Dict, previous_data: Optional[Dict]):
        """添加三档位图表的整体说明"""
        
        # 统计信息 - 修复去重问题
        all_brands = set()  # 使用集合去重
        for tier in ["20", "50", "100"]:
            if tier in current_data:
                tier_brands = {item['brand'] for item in current_data[tier]}
                all_brands.update(tier_brands)
        
        total_brands = len(all_brands)  # 去重后的品牌总数
        
        if previous_data:
            # 统计趋势变化
            trend_stats = {'上升': 0, '下降': 0, '新上榜': 0, '持平': 0}
            
            for tier in ["20", "50", "100"]:
                if tier in current_data and tier in previous_data:
                    current_brands = {item['brand']: float(item['sov_percentage']) 
                                    for item in current_data[tier]}
                    previous_brands = {item['brand']: float(item['sov_percentage']) 
                                     for item in previous_data[tier]}
                    
                    for brand, current_sov in current_brands.items():
                        if brand in previous_brands:
                            change = current_sov - previous_brands[brand]
                            if change > 0.1:
                                trend_stats['上升'] += 1
                            elif change < -0.1:
                                trend_stats['下降'] += 1
                            else:
                                trend_stats['持平'] += 1
                        else:
                            trend_stats['新上榜'] += 1
            
            annotation_text = (f"关键词 \"{keyword}\" SOV分析 | "
                             f"共{total_brands}个品牌上榜 | "
                             f"环比变化: {trend_stats['上升']}个上升, {trend_stats['下降']}个下降, "
                             f"{trend_stats['新上榜']}个新上榜, {trend_stats['持平']}个持平")
        else:
            annotation_text = f"关键词 \"{keyword}\" SOV分析 | 共{total_brands}个品牌上榜 | 暂无环比数据"
        
        # 数据来源说明
        date_str = datetime.now().strftime("%Y年%m月%d日")
        source_text = f"Copyright © 2025 乐见真言科技（苏州）有限公司 "
        
        # 添加文字说明 - 调整位置避免重叠
        # 主要说明文字放在底部留白区域
        fig.text(0.5, 0.08, annotation_text, ha='center', va='center', 
                fontsize=11, style='italic', wrap=True,
                fontproperties=fm.FontProperties(family=chinese_font),
                bbox=dict(boxstyle="round,pad=0.5", facecolor='lightyellow', alpha=0.8))
        
        # 版权信息放在最底部
        fig.text(0.5, 0.02, source_text, ha='center', va='bottom', 
                fontsize=9, color='gray',
                fontproperties=fm.FontProperties(family=chinese_font)) 