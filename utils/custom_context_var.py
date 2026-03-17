from contextvars import ContextVar
from typing import TypeVar

from typing_extensions import Generic


T = TypeVar('T')


"""
Simple wrapper on ContextVar that behaves exactly like ContextVar except it avoids calling .get() anytime to access
any field of the contextualized object
"""
class ContextVarWrapper(Generic[T]):

    def __init__(self, *args, **kwargs):
        self._ctxt_var: ContextVar[T] = ContextVar(*args, **kwargs)

    @property
    def value(self):
        return self._ctxt_var.get()

    @property
    def value_or_none(self):
        return self._ctxt_var.get(None)

    @property
    def name(self):
        return self._ctxt_var.name

    def __getattr__(self, item):
        if item == 'get':
            return self._ctxt_var.get
        elif item == 'set':
            return self._ctxt_var.set
        elif item == 'reset':
            return self._ctxt_var.reset
        return getattr(self._ctxt_var.get(), item)


if __name__ == '__main__':
    a = ContextVarWrapper('test')
    print(a.value_or_none)
    a.set(1)
    print(a.value)
    print(a.get())