"""
工具适配器 - 将新的服务层架构适配到原有的CrewAI工具接口
保持向后兼容性，同时利用新的分层架构优势
"""
import logging
from typing import Optional, Dict, Any
from crewai.tools import BaseTool

from ..services import (
    DataMergerService,
    SOVAnalysisService, 
    SentimentAnalysisService,
    SOVVisualizationService,
    SentimentVisualizationService
)

logger = logging.getLogger(__name__)

class DataMergerToolAdapter(BaseTool):
    """数据合并工具适配器"""
    name: str = "data_merger"
    description: str = "基于关键词从数据库查询搜索结果和笔记详情，合并生成宽表CSV文件"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.service = DataMergerService()
    
    def _run(self, keyword: str, output_dir_inner: str = "data/export", 
             output_dir_outer: str = "outputs") -> str:
        """执行数据合并"""
        return self.service.merge_data_for_keyword(
            keyword=keyword,
            output_dir_inner=output_dir_inner,
            output_dir_outer=output_dir_outer
        )

class SOVCalculatorToolAdapter(BaseTool):
    """SOV计算工具适配器"""
    name: str = "sov_calculator"
    description: str = "基于合并后的CSV数据，计算各品牌在指定关键词下的SOV（声量占比）"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.service = SOVAnalysisService()
    
    def _run(self, keyword: str, input_data_dir: str = "data/export", 
             output_data_dir: str = "outputs", method: str = "weighted") -> str:
        """执行SOV计算"""
        return self.service.calculate_sov(
            keyword=keyword,
            input_data_dir=input_data_dir,
            output_data_dir=output_data_dir,
            method=method
        )

class BrandSentimentExtractorAdapter(BaseTool):
    """品牌情感提取工具适配器"""
    name: str = "brand_sentiment_extractor"
    description: str = "基于指定的keyword和brand，从CSV文件中提取品牌情感倾向和高频词"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.service = SentimentAnalysisService()
    
    def _run(self, keyword: str, brand: str = "", output_filename: str = "", 
             csv_input_path: str = "") -> str:
        """执行品牌情感分析"""
        return self.service.extract_brand_sentiment(
            keyword=keyword,
            brand=brand,
            output_filename=output_filename,
            csv_input_path=csv_input_path
        )

class SOVVisualizationToolAdapter(BaseTool):
    """SOV可视化工具适配器"""
    name: str = "sov_visualization"
    description: str = "基于关键词生成SOV可视化图表，支持三档位(TOP20/TOP50/TOP100)对比"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.service = SOVVisualizationService()
    
    def _run(self, keyword: str, target_brand: str = "", output_dir: str = "outputs") -> str:
        """生成SOV可视化图表"""
        return self.service.generate_sov_charts(
            keyword=keyword,
            target_brand=target_brand,
            output_dir=output_dir
        )

class BrandSentimentVisualizationToolAdapter(BaseTool):
    """品牌情感分析可视化工具适配器"""
    name: str = "brand_sentiment_visualization"
    description: str = "基于关键词和品牌生成情感分析可视化图表"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.service = SentimentVisualizationService()
    
    def _run(self, keyword: str, target_brand: str, output_dir: str = "outputs") -> str:
        """生成品牌情感分析可视化图表"""
        return self.service.generate_sentiment_charts(
            keyword=keyword,
            target_brand=target_brand,
            output_dir=output_dir
        )

# 为了向后兼容，保留原有的类名
DataMergerTool = DataMergerToolAdapter
SOVCalculatorTool = SOVCalculatorToolAdapter
BrandSentimentExtractorTool = BrandSentimentExtractorAdapter
SOVVisualizationTool = SOVVisualizationToolAdapter
BrandSentimentVisualizationTool = BrandSentimentVisualizationToolAdapter 