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
            # 构建分析提示词
            prompt = self._build_analysis_prompt(content)
            
            # 调用LLM进行分析
            client = self._get_client()
            response = client.chat.completions.create(
               # model="deepseek/deepseek-r1-0528:free",  # 使用更稳定的模型
                model = "google/gemini-2.5-flash-preview",
                messages=[
                    {"role": "system", "content": self._get_system_prompt()},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0,  # 降低温度以减少幻觉
                max_tokens=1500,   # 适当降低token数，避免冗长输出
                top_p=0.9,        # 添加top_p参数进一步控制输出质量
                frequency_penalty=0.1  # 减少重复内容
            )
            
            analysis_result = response.choices[0].message.content
            logger.info(f"[AdvancedBrandAnalyzer] LLM原始输出: {analysis_result}")
            
            # 尝试解析为JSON
            parsed_result = self._parse_llm_result(analysis_result)
            
            # 如果解析成功，返回JSON字符串
            if not parsed_result.get('_analysis_failed', False):
                return json.dumps(parsed_result, ensure_ascii=False)
            else:
                # 解析失败，直接返回失败标记的JSON
                return json.dumps(parsed_result, ensure_ascii=False)
            
        except Exception as e:
            logger.error(f"[AdvancedBrandAnalyzer] 分析过程异常: {e}")
            # 返回异常标记的结果
            error_result = self._get_failed_result("analysis_exception", str(e))
            return json.dumps(error_result, ensure_ascii=False)
    
    def _parse_llm_result(self, llm_output: str) -> Dict[str, Any]:
        """解析LLM输出，返回成功结果或失败标记"""
        try:
            # 提取JSON字符串
            json_str = self._extract_json_string(llm_output)
            if not json_str:
                return self._get_failed_result("json_extraction_failed", "无法从LLM输出中提取JSON")
            
            # 解析JSON
            parsed = json.loads(json_str)
            if not isinstance(parsed, dict):
                return self._get_failed_result("invalid_format", "解析结果不是字典格式")
            
            # 标准化和清理格式
            result = self._standardize_and_clean(parsed)
            return result
            
        except json.JSONDecodeError as e:
            return self._get_failed_result("json_decode_error", f"JSON解析错误: {str(e)}")
        except Exception as e:
            return self._get_failed_result("parsing_error", f"解析过程出错: {str(e)}")
    
    def _extract_json_string(self, text: str) -> Optional[str]:
        """从文本中提取JSON字符串（简化版）"""
        text = text.strip()
        
        # 方法1: 检查是否为纯JSON
        if text.startswith('{') and text.endswith('}'):
            return text
        
        # 方法2: 查找```json```代码块
        json_match = re.search(r'```json\s*(.*?)\s*```', text, re.DOTALL)
        if json_match:
            return json_match.group(1).strip()
        
        # 方法3: 查找```代码块
        code_match = re.search(r'```\s*(.*?)\s*```', text, re.DOTALL)
        if code_match:
            content = code_match.group(1).strip()
            if content.startswith('{') and content.endswith('}'):
                return content
        
        # 方法4: 提取第一个完整的JSON对象
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
                    return text[start_pos:i+1]
        
        return None
    
    def _standardize_and_clean(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        """标准化和清理解析结果"""
        # 确保所有必需字段存在且类型正确
        result = {
            "brand_list": parsed.get("brand_list", []) if isinstance(parsed.get("brand_list"), list) else [],
            "spu_list": parsed.get("spu_list", []) if isinstance(parsed.get("spu_list"), list) else [],
            "emotion_dict": parsed.get("emotion_dict", {}) if isinstance(parsed.get("emotion_dict"), dict) else {},
            "evaluation_dict": parsed.get("evaluation_dict", {}) if isinstance(parsed.get("evaluation_dict"), dict) else {}
        }
        
        # 标准化情感标签
        emotion_mapping = {
            "positive": "正向", "正面": "正向", "好": "正向",
            "negative": "负向", "负面": "负向", "差": "负向", 
            "neutral": "中立", "一般": "中立", "普通": "中立"
        }
        
        for key, emotion in result["emotion_dict"].items():
            if emotion in emotion_mapping:
                result["emotion_dict"][key] = emotion_mapping[emotion]
            elif emotion not in ["正向", "负向", "中立"]:
                result["emotion_dict"][key] = "中立"
        
        # 确保评价词是列表
        for key, evaluations in result["evaluation_dict"].items():
            if not isinstance(evaluations, list):
                result["evaluation_dict"][key] = [evaluations] if isinstance(evaluations, str) else []
        
        return result
    
    def _get_failed_result(self, error_type: str, error_message: str) -> Dict[str, Any]:
        """返回标记为失败的结果"""
        return {
            "brand_list": [],
            "spu_list": [],
            "emotion_dict": {},
            "evaluation_dict": {},
            "_analysis_failed": True,
            "_error_type": error_type,
            "_error_message": error_message
        }
    
    def _get_system_prompt(self) -> str:
        """获取系统提示词"""
        return """你是一名专业的品牌分析和情感识别专家，特别擅长分析中文社交媒体内容。

**重要：你必须严格按照要求格式返回JSON，不得添加任何额外的文字、解释或其他内容！**

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

**输出格式要求：**
- 只返回纯JSON格式，不要包含```json```标记
- 不要添加任何解释文字或其他内容
- 严格按照以下JSON结构：

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
}"""

    def _build_analysis_prompt(self, content: str) -> str:
        """构建分析提示词"""
        return f"""请分析以下小红书笔记内容，严格按照要求格式返回JSON结果：

【待分析内容】
{content}

【分析要求】
1. 仔细识别其中提及的品牌和产品
2. 分析用户的情感倾向，注意真实情感态度
3. 提取最能代表用户观点的关键词汇
4. 如果没有明确提及品牌或产品，对应字段使用空列表

【关键要求】
- 只返回纯JSON格式，不要任何额外文字
- 不要使用代码块标记（如```json```）
- 不要添加解释或说明
- JSON必须是有效格式，可以被直接解析
- 情感值只能是："正向"、"负向"、"中立"

请立即返回JSON结果："""


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