from contextvars import ContextVar
from pathlib import Path


class InitContextVarException(Exception):
    def __init__(self, with_func_name: str, contextvar: ContextVar):
        super().__init__(f"This function call must be wrapped within previous `{contextvar.name}` context (created"
                         f"with `with {with_func_name}(...):`)")



current_fs_base = ContextVar[str]('current_fs_base', default=Path.home() / '.contextual')  # TODO: better context handling
