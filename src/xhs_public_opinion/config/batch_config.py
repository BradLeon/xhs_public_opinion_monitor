# -*- coding: utf-8 -*-
"""
批次处理配置文件

用于管理数据库读写和AI分析的批次大小参数
"""

class BatchConfig:
    """批次处理配置类"""
    
    # 数据库操作配置
    DB_READ_BATCH_SIZE = 100     # 数据库读取批次大小（可以较大，数据库能处理）
    DB_WRITE_MAX_SIZE = 200     # 数据库写入最大批次（批量写入的安全限制）
    
    # AI分析配置
    AI_BATCH_SIZE = 10          # AI分析批次大小（受LLM上下文长度限制）
    AI_MAX_CONTEXT_NOTES = 15   # 单次AI分析的最大笔记数量
    
    # 安全限制
    MAX_TOTAL_PROCESSING = 200  # 单次运行最大处理笔记数量
    
    @classmethod
    def get_config_summary(cls) -> str:
        """获取配置摘要"""
        return f"""
批次处理配置:
📊 数据库读取批次: {cls.DB_READ_BATCH_SIZE} 条
🤖 AI分析批次: {cls.AI_BATCH_SIZE} 条  
💾 数据库写入最大: {cls.DB_WRITE_MAX_SIZE} 条
⚠️ 单次运行最大: {cls.MAX_TOTAL_PROCESSING} 条
        """.strip()
    
    @classmethod
    def validate_and_adjust(cls, requested_db_size: int = None, requested_ai_size: int = None) -> tuple:
        """验证并调整批次大小"""
        db_size = requested_db_size or cls.DB_READ_BATCH_SIZE
        ai_size = requested_ai_size or cls.AI_BATCH_SIZE
        
        # 调整数据库批次大小
        if db_size > cls.MAX_TOTAL_PROCESSING:
            print(f"⚠️ 数据库批次大小 {db_size} 超过安全限制，调整为 {cls.MAX_TOTAL_PROCESSING}")
            db_size = cls.MAX_TOTAL_PROCESSING
        
        # 调整AI批次大小
        if ai_size > cls.AI_MAX_CONTEXT_NOTES:
            print(f"⚠️ AI批次大小 {ai_size} 超过上下文限制，调整为 {cls.AI_MAX_CONTEXT_NOTES}")
            ai_size = cls.AI_MAX_CONTEXT_NOTES
        
        if ai_size > db_size:
            print(f"⚠️ AI批次大小不能超过数据库批次大小，调整AI批次为 {db_size}")
            ai_size = db_size
        
        return db_size, ai_size 