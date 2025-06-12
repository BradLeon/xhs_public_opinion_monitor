"""
File Manager for XHS Public Opinion Monitor System

Unified file operations management for CSV, JSON, images and other file types.
Provides a clean interface for all file I/O operations used across the application.
"""

import os
import json
import glob
import logging
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime


logger = logging.getLogger(__name__)


class CSVManager:
    """CSV文件操作管理器"""
    
    @staticmethod
    def read_csv(file_path: str, encoding: str = 'utf-8-sig', **kwargs) -> pd.DataFrame:
        """
        读取CSV文件
        
        Args:
            file_path: CSV文件路径
            encoding: 文件编码，默认utf-8-sig
            **kwargs: pandas.read_csv的其他参数
            
        Returns:
            DataFrame对象
        """
        try:
            logger.info(f"读取CSV文件: {file_path}")
            return pd.read_csv(file_path, encoding=encoding, **kwargs)
        except Exception as e:
            logger.error(f"读取CSV文件失败 {file_path}: {e}")
            raise
    
    @staticmethod
    def write_csv(df: pd.DataFrame, file_path: str, encoding: str = 'utf-8-sig', 
                  index: bool = False, ensure_dir: bool = True, **kwargs) -> str:
        """
        写入CSV文件
        
        Args:
            df: 要写入的DataFrame
            file_path: 输出文件路径
            encoding: 文件编码，默认utf-8-sig
            index: 是否包含索引，默认False
            ensure_dir: 是否确保目录存在，默认True
            **kwargs: pandas.to_csv的其他参数
            
        Returns:
            写入的文件路径
        """
        try:
            if ensure_dir:
                DirectoryManager.ensure_directory(os.path.dirname(file_path))
                
            logger.info(f"写入CSV文件: {file_path}")
            df.to_csv(file_path, encoding=encoding, index=index, **kwargs)
            return file_path
        except Exception as e:
            logger.error(f"写入CSV文件失败 {file_path}: {e}")
            raise
    
    @staticmethod
    def find_latest_csv(pattern: str) -> Optional[str]:
        """
        根据glob模式查找最新的CSV文件
        
        Args:
            pattern: glob模式，如 "data/*.csv"
            
        Returns:
            最新文件的路径，如果没有找到则返回None
        """
        files = glob.glob(pattern)
        if not files:
            logger.warning(f"未找到匹配的CSV文件: {pattern}")
            return None
            
        latest_file = max(files, key=os.path.getctime)
        logger.info(f"找到最新CSV文件: {latest_file}")
        return latest_file


class JSONManager:
    """JSON数据处理管理器"""
    
    def read_json(self, file_path: str) -> Any:
        """读取JSON文件"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"读取JSON文件失败 {file_path}: {e}")
            raise
    
    def write_json(self, data: Any, file_path: str, ensure_ascii: bool = False, indent: int = 2) -> str:
        """写入JSON文件"""
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=ensure_ascii, indent=indent)
            
            logger.info(f"JSON文件保存成功: {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"保存JSON文件失败 {file_path}: {e}")
            raise
    
    def parse_json_string(self, json_str: str) -> Any:
        """解析JSON字符串"""
        try:
            if not json_str or json_str.strip() == "":
                return None
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.warning(f"JSON字符串解析失败: {json_str[:100]}... - {e}")
            return None
        except Exception as e:
            logger.error(f"JSON字符串解析异常: {e}")
            return None
    
    def to_json_string(self, data: Any, ensure_ascii: bool = False) -> str:
        """转换为JSON字符串"""
        try:
            return json.dumps(data, ensure_ascii=ensure_ascii)
        except Exception as e:
            logger.error(f"数据转JSON字符串失败: {e}")
            return "{}"


class ImageManager:
    """图像文件操作管理器"""
    
    @staticmethod
    def setup_matplotlib_chinese():
        """设置matplotlib中文字体支持"""
        try:
            # 尝试设置中文字体
            chinese_fonts = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
            for font_name in chinese_fonts:
                try:
                    plt.rcParams['font.sans-serif'] = [font_name]
                    plt.rcParams['axes.unicode_minus'] = False
                    plt.rcParams['font.size'] = 10
                    logger.info(f"成功设置字体: {font_name}")
                    break
                except:
                    continue
            
            # 设置matplotlib样式
            try:
                plt.style.use('seaborn-v0_8-whitegrid')
            except:
                plt.style.use('default')
                
        except Exception as e:
            logger.warning(f"设置matplotlib字体失败: {e}")
    
    @staticmethod
    def save_figure(fig, file_path: str, dpi: int = 300, 
                   bbox_inches: str = 'tight', facecolor: str = 'white',
                   edgecolor: str = 'none', ensure_dir: bool = True) -> str:
        """
        保存matplotlib图表
        
        Args:
            fig: matplotlib图表对象
            file_path: 输出文件路径
            dpi: 图片分辨率
            bbox_inches: 边界框设置
            facecolor: 背景色
            edgecolor: 边框色
            ensure_dir: 是否确保目录存在
            
        Returns:
            保存的文件路径
        """
        try:
            if ensure_dir:
                DirectoryManager.ensure_directory(os.path.dirname(file_path))
                
            logger.info(f"保存图表: {file_path}")
            plt.savefig(file_path, dpi=dpi, bbox_inches=bbox_inches, 
                       facecolor=facecolor, edgecolor=edgecolor)
            plt.close(fig)
            return file_path
        except Exception as e:
            logger.error(f"保存图表失败 {file_path}: {e}")
            raise
    
    @staticmethod
    def close_figure(fig=None):
        """关闭matplotlib图表"""
        try:
            if fig is not None:
                plt.close(fig)
            else:
                plt.close()
        except Exception as e:
            logger.warning(f"关闭图表时出现警告: {e}")


class DirectoryManager:
    """目录操作管理器"""
    
    @staticmethod
    def ensure_directory(dir_path: str) -> str:
        """
        确保目录存在，如果不存在则创建
        
        Args:
            dir_path: 目录路径
            
        Returns:
            目录路径
        """
        try:
            if dir_path and not os.path.exists(dir_path):
                os.makedirs(dir_path, exist_ok=True)
                logger.info(f"创建目录: {dir_path}")
            return dir_path
        except Exception as e:
            logger.error(f"创建目录失败 {dir_path}: {e}")
            raise
    
    @staticmethod
    def find_files(pattern: str) -> List[str]:
        """
        根据glob模式查找文件
        
        Args:
            pattern: glob模式
            
        Returns:
            匹配的文件路径列表
        """
        try:
            files = glob.glob(pattern)
            logger.info(f"找到 {len(files)} 个文件匹配模式: {pattern}")
            return files
        except Exception as e:
            logger.error(f"查找文件失败 {pattern}: {e}")
            raise
    
    @staticmethod
    def get_output_path(base_dir: str, filename: str, extension: str = None) -> str:
        """
        生成输出文件路径
        
        Args:
            base_dir: 基础目录
            filename: 文件名
            extension: 文件扩展名（可选）
            
        Returns:
            完整的输出文件路径
        """
        if extension and not filename.endswith(extension):
            filename = f"{filename}.{extension.lstrip('.')}"
            
        return os.path.join(base_dir, filename)


class TextFileManager:
    """文本文件操作管理器"""
    
    @staticmethod
    def read_text(file_path: str, encoding: str = 'utf-8') -> str:
        """
        读取文本文件
        
        Args:
            file_path: 文件路径
            encoding: 文件编码
            
        Returns:
            文件内容字符串
        """
        try:
            logger.info(f"读取文本文件: {file_path}")
            with open(file_path, 'r', encoding=encoding) as f:
                return f.read()
        except Exception as e:
            logger.error(f"读取文本文件失败 {file_path}: {e}")
            raise
    
    @staticmethod
    def write_text(content: str, file_path: str, encoding: str = 'utf-8',
                   ensure_dir: bool = True) -> str:
        """
        写入文本文件
        
        Args:
            content: 要写入的内容
            file_path: 输出文件路径
            encoding: 文件编码
            ensure_dir: 是否确保目录存在
            
        Returns:
            写入的文件路径
        """
        try:
            if ensure_dir:
                DirectoryManager.ensure_directory(os.path.dirname(file_path))
                
            logger.info(f"写入文本文件: {file_path}")
            with open(file_path, 'w', encoding=encoding) as f:
                f.write(content)
            return file_path
        except Exception as e:
            logger.error(f"写入文本文件失败 {file_path}: {e}")
            raise


class FileManager:
    """统一文件管理器 - 提供各种文件操作的统一接口"""
    
    def __init__(self):
        self.csv = CSVManager()
        self.json = JSONManager()
        self.image = ImageManager()
        self.directory = DirectoryManager()
        self.text = TextFileManager()
        
        # 设置matplotlib中文字体
        self.image.setup_matplotlib_chinese()
    
    def get_timestamped_filename(self, base_name: str, extension: str) -> str:
        """
        生成带时间戳的文件名
        
        Args:
            base_name: 基础文件名
            extension: 文件扩展名
            
        Returns:
            带时间戳的文件名
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{base_name}_{timestamp}.{extension}"
    
    def clean_temp_files(self, temp_dir: str, pattern: str = "*"):
        """
        清理临时文件
        
        Args:
            temp_dir: 临时文件目录
            pattern: 文件匹配模式
        """
        if os.path.exists(temp_dir):
            files = glob.glob(os.path.join(temp_dir, pattern))
            for file in files:
                try:
                    os.remove(file)
                    logger.info(f"删除临时文件: {file}")
                except Exception as e:
                    logger.error(f"删除临时文件失败 {file}: {e}")
    
    # === 为工具类提供的便捷方法 ===
    
    def find_files_by_pattern(self, pattern: str) -> List[str]:
        """根据模式查找文件"""
        return self.directory.find_files(pattern)
    
    def find_latest_file(self, files: List[str]) -> Optional[str]:
        """从文件列表中找到最新的文件"""
        if not files:
            return None
        return max(files, key=os.path.getctime)
    
    def parse_json_string(self, json_str: str) -> Any:
        """解析JSON字符串"""
        return self.json.parse_json_string(json_str)
    
    def to_json_string(self, data: Any, ensure_ascii: bool = False) -> str:
        """转换为JSON字符串"""
        return self.json.to_json_string(data, ensure_ascii=ensure_ascii)
    
    def build_path(self, *args) -> str:
        """构建文件路径"""
        return os.path.join(*args)
    
    def ensure_directory(self, dir_path: str) -> str:
        """确保目录存在"""
        return self.directory.ensure_directory(dir_path)
    
    def file_exists(self, file_path: str) -> bool:
        """检查文件是否存在"""
        return os.path.exists(file_path)
    
    def read_csv(self, file_path: str, **kwargs) -> pd.DataFrame:
        """读取CSV文件"""
        return self.csv.read_csv(file_path, **kwargs)
    
    def save_csv(self, df: pd.DataFrame, file_path: str, **kwargs) -> str:
        """保存CSV文件"""
        return self.csv.write_csv(df, file_path, **kwargs)
    
    def save_json(self, data: Dict[str, Any], file_path: str, **kwargs) -> str:
        """保存JSON文件"""
        return self.json.write_json(data, file_path, **kwargs)
    
    def save_chart(self, file_path: str, dpi: int = 300, **kwargs) -> str:
        """保存当前matplotlib图表"""
        return self.image.save_current_figure(file_path, dpi=dpi, **kwargs) 