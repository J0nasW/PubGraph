from alive_progress import alive_bar
from typing import ContextManager, Optional

def load_bar(title: Optional[str] = None) -> ContextManager:
    return alive_bar(monitor=None, stats=None, title=title)