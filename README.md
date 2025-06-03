# 小红书公共舆情分析系统

基于 CrewAI 框架构建的小红书笔记情感分析和品牌识别系统。

## 🎯 功能特点

- **智能品牌识别**: 自动识别笔记中提及的品牌和产品
- **情感分析**: 分析用户对品牌/产品的情感倾向（正向、负向、中立）
- **评价词提取**: 提取关键的描述词和评价词汇
- **多媒体内容分析**: 支持文本、图片和视频内容的综合分析
- **数据库集成**: 与 Supabase 数据库无缝集成
- **AI驱动**: 使用大语言模型进行高精度分析
- **🆕 批量处理优化**: 智能控制处理数量，避免上下文长度超限
- **🆕 内容长度自适应**: 自动截断过长内容，保持分析质量

## 🏗️ 系统架构

系统采用 CrewAI 多Agent协作架构，经过优化后使用更高效的两Agent设计：

1. **数据分析师 (Data Analyst)**: 负责从数据库读取笔记数据和预处理
2. **智能分析师 (Intelligent Analyst)**: 负责完整的品牌识别、情感分析和结果写入

## 📋 数据库表结构

系统分析 `public.xhs_note` 表的数据，主要字段包括：

**输入字段**:
- `title`: 笔记标题
- `desc`: 笔记描述
- `image_list`: 图片链接列表
- `video_url`: 视频链接（当type='video'时）
- `type`: 笔记类型

**输出字段**:
- `brand_list`: 识别的品牌列表
- `spu_list`: 识别的产品列表  
- `emotion_dict`: 情感分析结果
- `evaluation_dict`: 评价词汇

## 🚀 快速开始

### 1. 环境配置

```bash
# 克隆项目
cd xhs_public_opinion

# 安装依赖
pip install -e .

# 配置环境变量
cp env.example .env
```

在 `.env` 文件中配置：

```bash
# Supabase 数据库配置
SEO_SUPABASE_URL=your_supabase_url
SEO_SUPABASE_ANON_KEY=your_supabase_anon_key

# OpenAI API 配置（或使用OpenRouter）
OPENAI_API_KEY=your_openai_api_key
# 或者使用 OpenRouter
OPENROUTER_API_KEY=your_openrouter_api_key

# 可选：指定模型
OPENAI_MODEL_NAME=gpt-4-turbo-preview
```

### 2. 运行分析

```bash
# 运行完整分析流程
python -m xhs_public_opinion.main

# 或者直接使用命令
xhs_public_opinion
```

### 3. 其他命令

```bash
# 训练模型
python -m xhs_public_opinion.main train 10 training_data.json

# 重放任务
python -m xhs_public_opinion.main replay task_id

# 测试模型
python -m xhs_public_opinion.main test 5 gpt-4
```

## 📊 分析结果格式

系统生成的分析结果采用以下JSON格式：

```json
{
  "brand_list": ["兰蔻", "雅诗兰黛"],
  "spu_list": ["面霜", "精华液"],
  "emotion_dict": {
    "兰蔻": "正向",
    "精华液": "正向"
  },
  "evaluation_dict": {
    "兰蔻": ["好用", "温和", "效果显著"],
    "精华液": ["滋润", "吸收快", "性价比高"]
  }
}
```

## 🛠️ 核心工具

### 数据库工具
- `DatabaseReaderTool`: 读取未处理的笔记数据
- `DatabaseWriterTool`: 写入分析结果

### 内容分析工具  
- `ContentAnalyzerTool`: 基础内容分析
- `BrandRecognitionTool`: 品牌识别
- `EmotionAnalysisTool`: 情感分析
- `AdvancedBrandAnalyzer`: 基于LLM的高级分析
- `ContentSummarizer`: 内容摘要生成

## 📈 分析流程

1. **数据提取**: 从Supabase读取未处理的笔记
2. **内容分析**: 提取文本、图片、视频信息
3. **品牌识别**: 识别提及的品牌和产品
4. **情感分析**: 判断用户情感倾向
5. **结果处理**: 验证并写入数据库
6. **报告生成**: 生成分析报告

## 🔧 自定义配置

### 修改分析参数

在 `main.py` 中可以调整：

```python
inputs = {
    'analysis_type': '小红书笔记情感分析',
    'current_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    'batch_size': 10  # 每次处理的笔记数量
}
```

### 添加自定义品牌 

在 `content_analyzer.py` 中的 `common_brands` 列表添加新品牌：

```python
common_brands = [
    "兰蔻", "雅诗兰黛", "香奈儿", 
    # 添加新品牌
    "您的品牌名称"
]
```

## 📝 输出文件

- `analysis_report.md`: 详细分析报告
- 日志文件: 运行日志和错误信息

## 🐛 故障排除

### 常见问题

1. **环境变量未设置**
   ```
   错误：缺少必要的环境变量: SUPABASE_URL, SUPABASE_KEY, OPENAI_API_KEY
   ```
   解决：检查 `.env` 文件配置

2. **数据库连接失败**
   ```
   获取笔记数据失败: ...
   ```
   解决：验证 Supabase 配置和网络连接

3. **OpenAI API 错误**
   ```
   高级品牌分析失败: ...
   ```
   解决：检查 API 密钥和额度

### 日志查看

系统会输出详细的运行日志，包括：
- 📅 分析开始时间
- 📊 处理的笔记数量
- ✅ 成功完成的任务
- ❌ 错误信息和堆栈跟踪

## 🤝 贡献指南

欢迎提交 Issue 和 Pull Request 来改进项目！

## 📄 许可证

本项目采用 MIT 许可证。
