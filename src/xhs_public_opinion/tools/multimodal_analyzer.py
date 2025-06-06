import json
import re
import requests
import os
import logging
from typing import Dict, List, Any, Optional
from crewai.tools import BaseTool
from openai import OpenAI

logger = logging.getLogger(__name__)

class MultimodalBrandAnalyzer(BaseTool):
    """基于通义千问VL的多模态品牌分析工具"""
    name: str = "multimodal_brand_analyzer"
    description: str = "使用通义千问VL多模态大语言模型进行高级的品牌识别、产品分析和情感判断，支持文本、图片和视频内容"
    client: Optional[OpenAI] = None
    
    def __init__(self):
        """初始化多模态分析器"""
        super().__init__()
        self._init_openai_client()
        
    def _init_openai_client(self):
        """初始化OpenAI兼容客户端"""
        try:
            dashscope_api_key = os.getenv("DASHSCOPE_API_KEY")
            if not dashscope_api_key:
                raise ValueError("DASHSCOPE_API_KEY环境变量未设置")
            
            self.client = OpenAI(
                api_key=dashscope_api_key,
                base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
            )
            logger.info("✅ 通义千问VL客户端初始化成功")
                
        except Exception as e:
            logger.error(f"❌ 初始化通义千问VL客户端失败: {e}")
            self.client = None

    def _run(self, content: str, content_type: str) -> str:
        """执行多模态品牌分析"""
        try:
            if not self.client:
                return self._create_error_result("client_not_initialized", "客户端未初始化")
            
            # 解析输入内容
            note_data = self._parse_input_content(content)
            if not note_data:
                return self._create_error_result("input_parsing_failed", "无法解析输入内容")
            
            # test
            # logger.info(f"[MultimodalBrandAnalyzer] type of note_data: {type(note_data)}")
            logger.info(f"[MultimodalBrandAnalyzer] content_type: {content_type}")
            logger.info(f"[MultimodalBrandAnalyzer] note_data: {note_data}")
        
            # 构建多模态消息
            messages = self._build_multimodal_messages(note_data, content_type)
            
            # 调用Qwen-VL
            result = self._call_LLM(messages)
            logger.info(f"[MultimodalBrandAnalyzer] raw result: {result}")
            
            # 解析和标准化结果
            parsed_result = self._parse_llm_result(result)
            # logger.info(f"[MultimodalBrandAnalyzer] parsed result: {parsed_result}")

            return json.dumps(parsed_result, ensure_ascii=False)
            
        except Exception as e:
            logger.error(f"[MultimodalBrandAnalyzer] 分析过程异常: {e}")
            error_result = self._create_error_result("analysis_exception", str(e))
            return json.dumps(error_result, ensure_ascii=False)
    
    def _parse_input_content(self, content: str) -> Optional[Dict[str, Any]]:
        """解析输入内容，支持直接文本或JSON格式的笔记数据"""
        try:
            # 尝试解析为JSON
            note_data = json.loads(content)
            return note_data
        except json.JSONDecodeError:
            # 如果不是JSON，则作为纯文本处理
            return {
                'title': '',
                'desc': content,
                'image_list': [],
                'video_url': '',
                'type': 'normal'
            }
    
    def _build_multimodal_messages(self, note_data: Dict[str, Any], content_type: str) -> List[Dict[str, Any]]:
        """构建多模态消息格式"""
        # 构建文本内容
        text_content = self._build_text_prompt(note_data)
        
        # 构建消息内容列表
        content_parts = [
            {
                "type": "text",
                "text": text_content
            }
        ]
        
        # 处理视频内容
        if content_type == 'video' and note_data.get('type') == 'video' and note_data.get('video_url'):
            video_url = note_data['video_url']
            content_parts.append({
                "type": "video_url",
                "video_url": {
                    "url": video_url
                }
            })
        
        # 添加图片内容
        if content_type == 'image':
            image_list = note_data.get('image_list', '').split(',')
            if isinstance(image_list, list) and image_list:
                for image_url in image_list[:5]:  # 限制最多5张图片
                    if image_url and isinstance(image_url, str):
                        content_parts.append({
                            "type": "image_url",
                            "image_url": {
                                "url": image_url
                            }
                        })
            logger.info(f"[MultimodalBrandAnalyzer] image_list: {image_list}")
        
        messages = [
            {
                "role": "system",
                "content": self._get_system_prompt()
            },
            {
                "role": "user", 
                "content": content_parts
            }
        ]
        
        #logger.info(f"[MultimodalBrandAnalyzer] user messages: {content_parts}")

        return messages
    
    def _build_text_prompt(self, note_data: Dict[str, Any]) -> str:
        """构建文本分析提示"""
        title = note_data.get('title', '')
        desc = note_data.get('desc', '')
        
        prompt = f"""请综合分析以下小红书笔记的多模态内容，包括文本、图片和视频信息：

【文本内容】
【标题】: {title}
【文字】: {desc}


【分析要求】
1. 综合分析文本、图片和视频中的品牌信息
2. 识别所有提及的品牌和具体产品SPU
3. 判断用户对各品牌/产品的情感倾向
4. 提取关键的评价词汇
5. 特别关注图片和视频中的品牌展示、产品包装、使用场景等

【重要】只返回纯JSON格式，不要任何额外文字或代码块标记。"""
        
        
        return prompt
    
    def _call_LLM(self, messages: List[Dict[str, Any]]) -> str:
        """调用通义千问VL模型"""
        try:
            completion = self.client.chat.completions.create(
                model="qwen-vl-max-latest",
                messages=messages,
                temperature=0.1,
                max_tokens=2048,
                top_p=0.9
            )
            
            result = completion.choices[0].message.content
            logger.info(f"✅ 通义千问VL调用成功，返回内容长度: {len(result)}")
            return result
            
        except Exception as e:
            logger.error(f"❌ 通义千问VL调用失败: {e}")
            raise
    
    def _parse_llm_result(self, llm_output: str) -> Dict[str, Any]:
        """解析LLM输出"""
        try:
            # 提取JSON字符串
            json_str = self._extract_json_string(llm_output)
            if not json_str:
                return self._create_error_result("json_extraction_failed", "无法从LLM输出中提取JSON")
            
            # 解析JSON
            parsed = json.loads(json_str)
            if not isinstance(parsed, dict):
                return self._create_error_result("invalid_format", "解析结果不是字典格式")
            
            # 标准化和清理格式
            result = self._standardize_result(parsed)
            return result
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析错误: {e}")
            return self._create_error_result("json_decode_error", f"JSON解析错误: {str(e)}")
        except Exception as e:
            logger.error(f"解析过程出错: {e}")
            return self._create_error_result("parsing_error", f"解析过程出错: {str(e)}")
    
    def _extract_json_string(self, text: str) -> Optional[str]:
        """从文本中提取JSON字符串"""
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
    
    def _standardize_result(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        """标准化解析结果"""
        # 确保所有必需字段存在且类型正确
        result = {
            "brand_list": parsed.get("brand_list", []) if isinstance(parsed.get("brand_list"), list) else [],
            "spu_list": parsed.get("spu_list", []) if isinstance(parsed.get("spu_list"), list) else [],
            "emotion_dict": parsed.get("emotion_dict", {}) if isinstance(parsed.get("emotion_dict"), dict) else {},
            "evaluation_dict": parsed.get("evaluation_dict", {}) if isinstance(parsed.get("evaluation_dict"), dict) else {}
        }
        
        # 标准化情感标签
        emotion_mapping = {
            "positive": "正向", "正面": "正向", "好": "正向", "积极": "正向",
            "negative": "负向", "负面": "负向", "差": "负向", "消极": "负向",
            "neutral": "中立", "一般": "中立", "普通": "中立", "客观": "中立"
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
    
    def _create_error_result(self, error_type: str, error_message: str) -> Dict[str, Any]:
        """创建错误结果"""
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
        return """你是一名专业的多模态品牌分析和情感识别专家，特别擅长分析中文社交媒体内容（包括文本、图片和视频）。

**重要：你必须严格按照要求格式返回JSON，不得添加任何额外的文字、解释或其他内容！**

你的任务是综合分析小红书笔记的多模态内容，准确识别以下信息：

1. **多模态品牌识别**: 从文本、图片和视频中识别所有品牌，包括：
   - 文本中直接提及的品牌名称
   - 图片中的品牌LOGO、包装、标识
   - 视频中的品牌展示、产品演示和字幕信息（注意口播视频可能的镜像翻转效果导致品牌名、logo反向）
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

4. **评价词提取**: 提取用户对品牌/产品的具体评价词汇: 
   - 文本评价词
   - 视频口播中的评价表达
   - 视频展示效果的描述
   - 包括感受、使用效果、性能等

5. **视频内容分析**:
   - 产品展示场景
   - 使用步骤和方法
   - 效果对比展示
   - 包装细节展示
   - 品牌露出位置和时长

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
  },
  "video_analysis": {  
    "scenes": ["场景1", "场景2"],
    "brand_appearances": {
      "品牌1": ["时间点1", "时间点2"],
      "品牌2": ["时间点1", "时间点2"]
    },
    "key_moments": ["关键时刻1", "关键时刻2"]
  }
}""" 

