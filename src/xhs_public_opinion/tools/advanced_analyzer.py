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
            logger.info(f"[AdvancedBrandAnalyzer] LLM原始输出长度: {len(analysis_result)}")
            logger.info(f"[AdvancedBrandAnalyzer] LLM原始输出: {analysis_result}")
            
            # 预处理：检查是否包含明显的异常内容
            if self._contains_anomalous_content(analysis_result):
                logger.warning("[AdvancedBrandAnalyzer] 检测到异常内容，尝试重新处理")
                analysis_result = self._remove_anomalous_content(analysis_result)
            logger.info(f"[AdvancedBrandAnalyzer] after _remove_anomalous_content 输出: {analysis_result}")
            
            # 解析和验证结果
            parsed_result = self._parse_and_validate_json(analysis_result)
            logger.info(f"[AdvancedBrandAnalyzer] after _parse_and_validate_json 输出: {parsed_result}")

            
            # 最终JSON验证
            final_json = self._ensure_valid_json_output(parsed_result)
            
            logger.info(f"[AdvancedBrandAnalyzer] after _ensure_valid_json_output 最终输出: {final_json}")

            return final_json
            
        except Exception as e:
            logger.error(f"[AdvancedBrandAnalyzer] 高级品牌分析失败: {e}")
            # 返回标记失败的结果，包含错误信息
            error_result = {
                "brand_list": [],
                "spu_list": [],
                "emotion_dict": {},
                "evaluation_dict": {},
                "_analysis_failed": True,
                "_error_message": str(e),
                "_error_type": "analysis_exception"
            }
            return json.dumps(error_result, ensure_ascii=False)
    
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
            # 返回标记失败的结果
            error_result = self._get_failed_result("json_validation_error", f"JSON格式验证失败: {str(e)}")
            return json.dumps(error_result, ensure_ascii=False, separators=(',', ':'))
    
    def _parse_and_validate_json(self, result: str) -> Dict[str, Any]:
        """解析和验证LLM返回的分析结果"""
        try:
            logger.info(f"[AdvancedBrandAnalyzer] 开始解析LLM输出，长度: {len(result)}")
            
            # 尝试多种JSON提取方法
            json_str = self._extract_json_from_text(result)
            
            if not json_str:
                logger.warning("[AdvancedBrandAnalyzer] 无法提取JSON，返回失败标记")
                return self._get_failed_result("json_extraction_failed", "无法从LLM输出中提取有效JSON")
            
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
            return self._get_failed_result("json_decode_error", f"JSON解析错误: {str(e)}")
        except Exception as e:
            logger.error(f"[AdvancedBrandAnalyzer] 其他解析错误: {e}")
            return self._get_failed_result("parsing_error", f"解析过程出错: {str(e)}")
    
    def _extract_json_from_text(self, text: str) -> Optional[str]:
        """从文本中提取JSON"""
        # 清理文本，移除可能的干扰字符
        text = text.strip()
        
        # 方法1: 直接检查是否为纯JSON（优先级最高）
        if text.startswith('{') and text.endswith('}'):
            logger.info("[AdvancedBrandAnalyzer] 检测到纯JSON格式")
            # 检查是否包含无关内容，如果有则尝试提取纯JSON部分
            brace_count = 0
            start_pos = 0
            for i, char in enumerate(text):
                if char == '{':
                    if brace_count == 0:
                        start_pos = i
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        json_candidate = text[start_pos:i+1]
                        try:
                            # 验证是否为有效JSON
                            json.loads(json_candidate)
                            logger.info(f"[AdvancedBrandAnalyzer] 验证JSON有效，长度: {len(json_candidate)}")
                            return json_candidate
                        except:
                            continue
            return text  # 如果验证失败，返回原文本
        
        # 方法2: 查找```json```代码块
        json_match = re.search(r'```json\s*(.*?)\s*```', text, re.DOTALL)
        if json_match:
            logger.info("[AdvancedBrandAnalyzer] 找到```json```代码块")
            candidate = json_match.group(1).strip()
            # 进一步清理可能的干扰内容
            candidate = self._clean_json_candidate(candidate)
            return candidate
        
        # 方法3: 查找```代码块（不指定语言）
        code_match = re.search(r'```\s*(.*?)\s*```', text, re.DOTALL)
        if code_match:
            content = code_match.group(1).strip()
            if content.startswith('{') and content.endswith('}'):
                logger.info("[AdvancedBrandAnalyzer] 找到```代码块中的JSON")
                content = self._clean_json_candidate(content)
                return content
        
        # 方法4: 尝试从文本中提取第一个完整的JSON对象
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
                    json_candidate = self._clean_json_candidate(json_candidate)
                    logger.info(f"[AdvancedBrandAnalyzer] 找到JSON对象候选: {json_candidate[:100]}...")
                    return json_candidate
        
        logger.warning("[AdvancedBrandAnalyzer] 无法从文本中提取JSON")
        return None
    
    def _clean_json_candidate(self, json_str: str) -> str:
        """清理JSON候选字符串，移除可能的干扰内容"""
        # 移除常见的干扰文本模式
        patterns_to_remove = [
            r'chimneys are a common feature.*?furnaces\.',
            r'[A-Za-z\s]+are a common feature.*?\.',
            r'[A-Za-z\s]+providing ventilation.*?\.',
            # 添加更多可能的干扰模式
            r'\n[A-Za-z\s]{20,}?\.',  # 长英文句子
            r'[A-Z][a-z]+\s+[a-z\s]{10,}?\.',  # 英文句子模式
        ]
        
        cleaned = json_str
        for pattern in patterns_to_remove:
            cleaned = re.sub(pattern, '', cleaned, flags=re.DOTALL | re.IGNORECASE)
        
        # 清理多余的空白和换行
        cleaned = re.sub(r'\s+', ' ', cleaned)
        cleaned = cleaned.strip()
        
        # 如果清理后的内容与原内容不同，记录日志
        if cleaned != json_str:
            logger.info(f"[AdvancedBrandAnalyzer] JSON清理: 原长度{len(json_str)} -> 清理后长度{len(cleaned)}")
        
        return cleaned
    
    def _standardize_result_format(self, parsed: Any) -> Dict[str, Any]:
        """标准化解析结果格式"""
        if not isinstance(parsed, dict):
            logger.warning(f"[AdvancedBrandAnalyzer] 解析结果不是字典格式: {type(parsed)}")
            return self._get_failed_result("invalid_result_format", "解析结果不是有效的字典格式")
        
        # 确保所有必需字段存在
        result = {
            "brand_list": parsed.get("brand_list", []),
            "spu_list": parsed.get("spu_list", []),
            "emotion_dict": parsed.get("emotion_dict", {}),
            "evaluation_dict": parsed.get("evaluation_dict", {})
        }
        
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
    
    def _get_empty_result(self) -> Dict[str, Any]:
        """返回空结果（保留兼容性）"""
        return self._get_failed_result("empty_result", "分析结果为空")
    
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
    
    def _contains_anomalous_content(self, text: str) -> bool:
        """检测文本是否包含异常内容"""
        anomalous_patterns = [
            r'chimneys are a common feature',
            r'providing ventilation for',
            r'[A-Z][a-z]+\s+are a common feature',
            r'buildings.*ventilation.*furnaces',
            # 检测过长的英文句子（可能是幻觉）
            r'[A-Za-z\s]{50,}\.', 
        ]
        
        for pattern in anomalous_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                logger.warning(f"[AdvancedBrandAnalyzer] 检测到异常模式: {pattern}")
                return True
        return False
    
    def _remove_anomalous_content(self, text: str) -> str:
        """移除异常内容"""
        # 更激进的清理策略
        anomalous_patterns = [
            r'chimneys are a common feature.*?furnaces\.',
            r'[A-Za-z\s]+are a common feature.*?\.',
            r'[A-Za-z\s]+providing ventilation.*?\.',
            r'buildings.*?ventilation.*?furnaces.*?\.',
            # 移除长英文句子
            r'[A-Z][a-z\s]{30,}?\.',
        ]
        
        cleaned = text
        for pattern in anomalous_patterns:
            old_length = len(cleaned)
            cleaned = re.sub(pattern, '', cleaned, flags=re.DOTALL | re.IGNORECASE)
            if len(cleaned) != old_length:
                logger.info(f"[AdvancedBrandAnalyzer] 移除异常内容，长度从 {old_length} 变为 {len(cleaned)}")
        
        return cleaned

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