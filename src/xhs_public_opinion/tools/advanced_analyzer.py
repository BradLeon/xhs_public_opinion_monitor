import json
import re
from typing import Dict, List, Any, Optional
from crewai.tools import BaseTool
import requests
import openai
import os
import logging

logger = logging.getLogger(__name__)

class AdvancedBrandAnalyzer(BaseTool):
    """基于LLM的高级品牌分析工具"""
    name: str = "advanced_brand_analyzer"
    description: str = "使用大语言模型进行高级的品牌识别、产品分析和情感判断"
    
    def _get_client(self):
        """延迟初始化OpenAI客户端"""
        if not hasattr(self, '_client'):
            self._client = openai.OpenAI(
                api_key=os.getenv("OPENROUTER_API_KEY"),
                base_url="https://openrouter.ai/api/v1"
            )
        return self._client
        
    def _run(self, content: str) -> str:
        """执行高级品牌分析"""
        try:
            # 内容长度优化
            #optimized_content = self._optimize_content_length(content)
            
            # 构建分析提示词
            # print(f"执行高级品牌分析 input content: {content}")

            prompt = self._build_analysis_prompt(content)
            
            # 调用LLM进行分析
            client = self._get_client()

            response = client.chat.completions.create(
                model = "google/gemini-2.5-flash-preview",
                #model="deepseek/deepseek-r1-0528:free",
                messages=[
                    {"role": "system", "content": self._get_system_prompt()},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=2000
            )
            
            analysis_result = response.choices[0].message.content
            #logger.info(f"[AdvancedBrandAnalyzer] LLM原始输出: {analysis_result}")
            
            # 解析和验证结果
            parsed_result = self._parse_and_validate_json(analysis_result)
            
            # 最终JSON验证
            final_json = self._ensure_valid_json_output(parsed_result)
            
            #logger.info(f"[AdvancedBrandAnalyzer] 最终输出: {final_json}")
            
            return final_json
            
        except Exception as e:
            logger.error(f"高级品牌分析失败: {e}")
            # 返回标准的空结果JSON
            empty_result = {
                "brand_list": [],
                "spu_list": [],
                "emotion_dict": {},
                "evaluation_dict": {}
            }
            return json.dumps(empty_result, ensure_ascii=False)
    
    def _ensure_valid_json_output(self, parsed_result: Dict[str, Any]) -> str:
        """确保输出是有效的JSON格式"""
        try:
            # 生成JSON字符串
            json_output = json.dumps(parsed_result, ensure_ascii=False, separators=(',', ':'))
            
            # 验证JSON是否可以被正确解析
            json.loads(json_output)
            
            logger.info(f"[AdvancedBrandAnalyzer] JSON格式验证成功，长度: {len(json_output)}")
            return json_output
            
        except Exception as e:
            logger.error(f"[AdvancedBrandAnalyzer] JSON格式验证失败: {e}")
            # 返回标准的空结果
            empty_result = {
                "brand_list": [],
                "spu_list": [],
                "emotion_dict": {},
                "evaluation_dict": {}
            }
            return json.dumps(empty_result, ensure_ascii=False, separators=(',', ':'))
    
    def _parse_and_validate_json(self, result: str) -> Dict[str, Any]:
        """解析和验证LLM返回的分析结果"""
        try:
            logger.info(f"[AdvancedBrandAnalyzer] 开始解析LLM输出，长度: {len(result)}")
            
            # 尝试多种JSON提取方法
            json_str = self._extract_json_from_text(result)
            
            if not json_str:
                logger.warning("[AdvancedBrandAnalyzer] 无法提取JSON，返回空结果")
                return self._get_empty_result()
            
            # 尝试解析JSON
            parsed = json.loads(json_str)
            logger.info(f"[AdvancedBrandAnalyzer] JSON解析成功: {parsed}")
            
            # 验证和标准化结果格式
            standardized = self._standardize_result_format(parsed)
            
            # 清理和验证数据
            cleaned = self._clean_analysis_result(standardized)
            
            return cleaned
            
        except json.JSONDecodeError as e:
            logger.error(f"[AdvancedBrandAnalyzer] JSON解析失败: {e}")
            logger.error(f"[AdvancedBrandAnalyzer] 问题位置: {e.pos if hasattr(e, 'pos') else 'unknown'}")
            logger.error(f"[AdvancedBrandAnalyzer] 原始内容: {result}")
            return self._get_empty_result()
        except Exception as e:
            logger.error(f"[AdvancedBrandAnalyzer] 其他解析错误: {e}")
            return self._get_empty_result()
    
    def _extract_json_from_text(self, text: str) -> Optional[str]:
        """从文本中提取JSON"""
        # 方法1: 查找```json```代码块
        json_match = re.search(r'```json\s*(.*?)\s*```', text, re.DOTALL)
        if json_match:
            logger.info("[AdvancedBrandAnalyzer] 找到```json```代码块")
            return json_match.group(1).strip()
        
        # 方法2: 查找```代码块（不指定语言）
        code_match = re.search(r'```\s*(.*?)\s*```', text, re.DOTALL)
        if code_match:
            content = code_match.group(1).strip()
            if content.startswith('{') and content.endswith('}'):
                logger.info("[AdvancedBrandAnalyzer] 找到```代码块中的JSON")
                return content
        
        # 方法3: 查找第一个完整的JSON对象
        brace_count = 0
        start_pos = -1
        for i, char in enumerate(text):
            if char == '{':
                if brace_count == 0:
                    start_pos = i
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0 and start_pos != -1:
                    json_candidate = text[start_pos:i+1]
                    logger.info(f"[AdvancedBrandAnalyzer] 找到JSON对象候选: {json_candidate[:100]}...")
                    return json_candidate
        
        # 方法4: 尝试整个文本（去除前后空白）
        text_stripped = text.strip()
        if text_stripped.startswith('{') and text_stripped.endswith('}'):
            logger.info("[AdvancedBrandAnalyzer] 使用整个文本作为JSON")
            return text_stripped
        
        logger.warning("[AdvancedBrandAnalyzer] 无法从文本中提取JSON")
        return None
    
    def _standardize_result_format(self, parsed: Any) -> Dict[str, Any]:
        """标准化解析结果格式"""
        if not isinstance(parsed, dict):
            logger.warning(f"[AdvancedBrandAnalyzer] 解析结果不是字典格式: {type(parsed)}")
            return self._get_empty_result()
        
        # 确保所有必需字段存在
        result = {
            "brand_list": parsed.get("brand_list", []),
            "spu_list": parsed.get("spu_list", []),
            "emotion_dict": parsed.get("emotion_dict", {}),
            "evaluation_dict": parsed.get("evaluation_dict", {})
        }
        
        return result
    
    def _get_empty_result(self) -> Dict[str, Any]:
        """获取空的标准结果"""
        return {
            "brand_list": [],
            "spu_list": [],
            "emotion_dict": {},
            "evaluation_dict": {}
        }
    
    def _optimize_content_length(self, content: str, max_length: int = 8000) -> str:
        """优化内容长度，避免上下文过长"""
        if len(content) <= max_length:
            return content
            
        logger.warning(f"[AdvancedBrandAnalyzer] 内容长度超限 ({len(content)} > {max_length})，进行截断")
        
        # 尝试解析JSON并智能截断
        try:
            data = json.loads(content)
            if isinstance(data, dict) and 'notes' in data:
                notes = data['notes']
                truncated_notes = []
                current_length = 0
                
                for note in notes:
                    note_str = json.dumps(note, ensure_ascii=False)
                    if current_length + len(note_str) > max_length - 500:  # 预留500字符
                        break
                    truncated_notes.append(note)
                    current_length += len(note_str)
                
                data['notes'] = truncated_notes
                logger.info(f"[AdvancedBrandAnalyzer] 智能截断保留 {len(truncated_notes)} 条笔记")
                return json.dumps(data, ensure_ascii=False)
                
        except:
            pass
        
        # 简单截断
        truncated = content[:max_length]
        logger.info(f"[AdvancedBrandAnalyzer] 简单截断到 {len(truncated)} 字符")
        return truncated
    
    def _get_system_prompt(self) -> str:
        """获取系统提示词"""
        return """你是一名专业的品牌分析和情感识别专家，特别擅长分析中文社交媒体内容。

你的任务是分析小红书笔记内容，准确识别以下信息：

1. **品牌识别**: 识别文本中提及的所有品牌名称，包括：
   - 国际大牌（如兰蔻、雅诗兰黛、香奈儿等）
   - 国产品牌（如完美日记、花西子、珀莱雅等）  
   - 小众品牌和新兴品牌
   - 注意品牌的不同表达方式（缩写、昵称等）

2. **spu识别**: 识别具体的产品SPU，注意与品牌、产品类型的区别。如：
   - SK-II神仙水
   - 雅诗兰黛小圆管唇蜜
   - 雅诗兰黛沁水粉底液
   - 花西子同心锁口红
   - 珀莱雅源力水乳


3. **情感分析**: 判断用户对每个品牌/产品的情感倾向：
   - 正向：喜欢、推荐、效果好等
   - 负向：不喜欢、不推荐、效果差等  
   - 中立：一般、还行、客观描述等

4. **评价词提取**: 提取用户对品牌/产品的具体评价词汇

请以JSON格式返回分析结果，严格按照以下格式：
```json
{
  "brand_list": ["品牌1", "品牌2"],
  "spu_list": ["spu1", "spu2"], 
  "emotion_dict": {
    "品牌1": "正向/负向/中立",
    "spu1": "正向/负向/中立"
  },
  "evaluation_dict": {
    "品牌1": ["评价词1", "评价词2"],
    "spu1": ["评价词1", "评价词2"]
  }
}
```"""

    def _build_analysis_prompt(self, content: str) -> str:
        """构建分析提示词"""
        return f"""请分析以下小红书笔记内容：

{content}

请仔细识别其中提及的品牌和产品，分析用户的情感倾向，并提取关键评价词汇。

注意事项：
1. 仔细区分品牌名称和产品类型
2. 注意用户的真实情感态度，不要被表面词汇误导
3. 提取最能代表用户观点的关键词汇
4. 如果没有明确提及品牌或产品，对应字段可以为空列表

请严格按照JSON格式返回结果。"""

    def _parse_analysis_result(self, result: str) -> Dict[str, Any]:
        """解析LLM返回的分析结果"""
        try:
            # 尝试提取JSON部分
            json_match = re.search(r'```json\s*(.*?)\s*```', result, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # 如果没有```json```包装，尝试直接解析
                json_str = result.strip()
            
            parsed = json.loads(json_str)

            # print(f"[AdvancedBrandAnalyzer._parse_analysis_result] 解析结果: {parsed}")
            
            # 验证和标准化结果格式
            standardized = {
                "brand_list": parsed.get("brand_list", []),
                "spu_list": parsed.get("spu_list", []),
                "emotion_dict": parsed.get("emotion_dict", {}),
                "evaluation_dict": parsed.get("evaluation_dict", {})
            }
            
            # 清理和验证数据
            standardized = self._clean_analysis_result(standardized)
            
            return standardized
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {e}, 原始结果: {result}")
            # 返回空结果
            return {
                "brand_list": [],
                "spu_list": [],
                "emotion_dict": {},
                "evaluation_dict": {}
            }
    
    def _clean_analysis_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """清理和验证分析结果"""
        # 确保列表类型
        if not isinstance(result.get("brand_list"), list):
            result["brand_list"] = []
        
        if not isinstance(result.get("spu_list"), list):
            result["spu_list"] = []
        
        # 确保字典类型
        if not isinstance(result.get("emotion_dict"), dict):
            result["emotion_dict"] = {}
            
        if not isinstance(result.get("evaluation_dict"), dict):
            result["evaluation_dict"] = {}
        
        # 标准化情感标签
        emotion_mapping = {
            "positive": "正向", "正面": "正向", "好": "正向", "推荐": "正向",
            "negative": "负向", "负面": "负向", "差": "负向", "不推荐": "负向", 
            "neutral": "中立", "一般": "中立", "普通": "中立"
        }
        
        for key, emotion in result["emotion_dict"].items():
            if emotion in emotion_mapping:
                result["emotion_dict"][key] = emotion_mapping[emotion]
            elif emotion not in ["正向", "负向", "中立"]:
                result["emotion_dict"][key] = "中立"  # 默认为中立
        
        # 确保评价词是列表
        for key, evaluations in result["evaluation_dict"].items():
            if not isinstance(evaluations, list):
                if isinstance(evaluations, str):
                    result["evaluation_dict"][key] = [evaluations]
                else:
                    result["evaluation_dict"][key] = []
        
        return result
    
# todo(liuchao), 这里做内容理解信息提取，需要区分内容类型是文字、图片还是视频。 不同的内容类型，需要不同的模型请求方式。
class ContentSummarizer(BaseTool):
    """内容摘要生成工具"""
    name: str = "content_summarizer"
    description: str = "生成小红书笔记内容的摘要和关键信息提取"
    
    def _get_client(self):
        """延迟初始化OpenAI客户端"""
        if not hasattr(self, '_client'):
            self._client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        return self._client
    
    def _run(self, content: str) -> str:
        """生成内容摘要"""
        try:
            prompt = f"""请对以下小红书笔记内容进行摘要，提取关键信息：

{content}

请从以下几个维度进行分析：
1. 内容主题（护肤、彩妆、生活方式等）
2. 主要讨论的产品或话题
3. 用户的主要观点和态度
4. 内容的情感基调

请用简洁的中文回答。"""

            client = self._get_client()
            response = client.chat.completions.create(
                model=os.getenv("OPENAI_MODEL_NAME", "gpt-4-turbo-preview"),
                messages=[
                    {"role": "system", "content": "你是一名专业的内容分析师，擅长提取和总结社交媒体内容的核心信息。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=500
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"内容摘要生成失败: {e}")
            return f"摘要生成失败: {str(e)}" 