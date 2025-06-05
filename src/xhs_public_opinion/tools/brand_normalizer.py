import re
import json
from typing import Dict, List, Optional
from difflib import SequenceMatcher
import logging

logger = logging.getLogger(__name__)

class BrandNormalizer:
    """品牌名标准化工具"""
    
    def __init__(self, custom_mapping: Optional[Dict[str, str]] = None):
        """
        初始化品牌名标准化器
        
        Args:
            custom_mapping: 自定义品牌名映射表
        """
        # 默认品牌名映射表（可扩展）
        self.brand_mapping = {
            # 大小写和格式变体
            "living proof": "Living Proof",
            "offrelax": "Off&Relax", 
            "fanbeauty": "Fan Beauty",
            "fan beauty": "Fan Beauty",
            "kérastase": "Kérastase",
            "kerastase": "Kérastase",
            "christophe robin": "Christophe Robin",
            "rene furterer": "René Furterer",
            "my.organics": "MY.ORGANICS",
            
            # 中英文对应
            "缕灵": "Living Proof",
            "卡诗": "Kérastase", 
            "欧莱雅": "L'Oréal",
            "资生堂": "Shiseido",
            "科颜氏": "Kiehl's",
            
            # 常见变体
            "潘婷": "Pantene",
            "海飞丝": "Head & Shoulders",
            "多芬": "Dove",
            "力士": "Lux",
            "清扬": "Clear",
            
        }
        
        # 合并自定义映射
        if custom_mapping:
            self.brand_mapping.update(custom_mapping)
            
        # 预编译正则表达式
        self.cleanup_pattern = re.compile(r'[^\w\s&\'-]', re.UNICODE)
        self.space_pattern = re.compile(r'\s+')
    
    def normalize_brand_name(self, brand_name: str) -> str:
        """
        标准化品牌名
        
        Args:
            brand_name: 原始品牌名
            
        Returns:
            标准化后的品牌名
        """
        if not brand_name or not isinstance(brand_name, str):
            return ""
        
        # 1. 基础清理
        normalized = self._basic_cleanup(brand_name)
        
        # 2. 直接映射查找
        mapped_name = self._direct_mapping(normalized)
        if mapped_name:
            return mapped_name
        
        # 3. 相似度匹配（可选）
        similar_name = self._similarity_matching(normalized)
        if similar_name:
            return similar_name
            
        # 4. 返回清理后的名称
        return self._format_final_name(normalized)
    
    def _basic_cleanup(self, brand_name: str) -> str:
        """基础字符串清理"""
        # 去除首尾空格
        cleaned = brand_name.strip()
        
        # 去除多余的特殊字符（保留常见的&, ', -）
        cleaned = self.cleanup_pattern.sub(' ', cleaned)
        
        # 标准化空格
        cleaned = self.space_pattern.sub(' ', cleaned).strip()
        
        return cleaned
    
    def _direct_mapping(self, normalized_name: str) -> Optional[str]:
        """直接映射查找"""
        # 尝试不同的匹配方式
        lookup_variants = [
            normalized_name,
            normalized_name.lower(),
            normalized_name.upper(),
            normalized_name.title(),
        ]
        
        for variant in lookup_variants:
            if variant in self.brand_mapping:
                logger.debug(f"品牌映射: {normalized_name} -> {self.brand_mapping[variant]}")
                return self.brand_mapping[variant]
        
        return None
    
    def _similarity_matching(self, normalized_name: str, threshold: float = 0.8) -> Optional[str]:
        """相似度匹配"""
        best_match = None
        best_score = 0
        
        normalized_lower = normalized_name.lower()
        
        for key, standard_name in self.brand_mapping.items():
            # 计算相似度
            score = SequenceMatcher(None, normalized_lower, key.lower()).ratio()
            
            if score > threshold and score > best_score:
                best_score = score
                best_match = standard_name
        
        if best_match:
            logger.debug(f"相似度匹配: {normalized_name} -> {best_match} (score: {best_score:.3f})")
        
        return best_match
    
    def _format_final_name(self, cleaned_name: str) -> str:
        """格式化最终品牌名"""
        # 基本的首字母大写处理
        words = cleaned_name.split()
        formatted_words = []
        
        for word in words:
            if word.upper() in ['&', 'AND', 'OF', 'THE']:
                formatted_words.append(word.upper() if word.upper() == '&' else word.lower())
            else:
                formatted_words.append(word.capitalize())
        
        return ' '.join(formatted_words)
    
    def normalize_brand_list(self, brand_list: List[str]) -> List[str]:
        """批量标准化品牌名列表"""
        normalized_brands = []
        seen_brands = set()
        
        for brand in brand_list:
            normalized = self.normalize_brand_name(brand)
            if normalized and normalized not in seen_brands:
                normalized_brands.append(normalized)
                seen_brands.add(normalized)
        
        return normalized_brands
    
    def add_brand_mapping(self, variants: List[str], standard_name: str):
        """添加品牌映射"""
        for variant in variants:
            self.brand_mapping[variant.lower()] = standard_name
    
    def get_mapping_stats(self) -> Dict[str, int]:
        """获取映射统计信息"""
        return {
            "total_mappings": len(self.brand_mapping),
            "unique_standard_names": len(set(self.brand_mapping.values()))
        }
    
    def export_mapping(self, filepath: str):
        """导出品牌映射表"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.brand_mapping, f, ensure_ascii=False, indent=2)
    
    def load_mapping(self, filepath: str):
        """加载品牌映射表"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                loaded_mapping = json.load(f)
                self.brand_mapping.update(loaded_mapping)
                logger.info(f"已加载品牌映射表: {filepath}")
        except FileNotFoundError:
            logger.warning(f"品牌映射表文件不存在: {filepath}")
        except Exception as e:
            logger.error(f"加载品牌映射表失败: {e}")


# 全局品牌标准化器实例
_global_normalizer = None

def get_brand_normalizer() -> BrandNormalizer:
    """获取全局品牌标准化器实例"""
    global _global_normalizer
    if _global_normalizer is None:
        _global_normalizer = BrandNormalizer()
        # 尝试加载自定义映射
        try:
            _global_normalizer.load_mapping("config/brand_mapping.json")
        except:
            pass
    return _global_normalizer 