"""
Data Store Layer for XHS Public Opinion Monitor System

This module provides unified data access interfaces for:
- Database operations (SupabaseDatabase)
- File operations (FileManager)
"""

from .database import SupabaseDatabase, XHSNote
from .file_manager import FileManager

__all__ = ['SupabaseDatabase', 'XHSNote', 'FileManager'] 