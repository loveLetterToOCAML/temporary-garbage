from contextvars import ContextVar
from typing import TypeVar

from typing_extensions import Generic

from baseimplems.anyio_utils import NotInAsyncContextManager, AsyncContextManagerDependencyNotEntered

T = TypeVar('T')


"""
Simple wrapper on ContextVar that behaves exactly like ContextVar except it avoids calling .get() anytime to access
any field of the contextualized object
"""
class ContextVarWrapper(Generic[T]):
    # singleton to avoid 1 different contextvar object per a given name
    # warning: this induces extra-precaution as contextvars handle unicity nicely, while name-keying introduces copy paste risks
    # otherwise we don't have the insurance the module imported var will be imported the same and match the exact same object in memory
    _registry: dict[str, ContextVar] = {}

    def __init__(self, name, run_within_proposal: str | None = None, *args, **kwargs):
        if name not in self._registry:
            self._registry[name] = ContextVar[T](name, *args, **kwargs)
        self._ctxt_var: ContextVar[T] = self._registry[name]
        self._run_within_proposal = run_within_proposal

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
        try:
            resolved = self._ctxt_var.get()
        except LookupError:
            raise AsyncContextManagerDependencyNotEntered(self._ctxt_var, self._run_within_proposal or self._ctxt_var.name, item)
        return getattr(resolved, item)


class ContextVarPropertyWrapper(Generic[T]):

    def __init__(self, ctxt_var: ContextVar[T] | ContextVarWrapper[T], attr_name: str):
        self._ctxt_var = ctxt_var
        self._attr = attr_name

    def get(self):
        return getattr(self._ctxt_var.get(), self._attr)


if __name__ == '__main__':
    a = ContextVarWrapper('test')
    print(a.value_or_none)
    a.set(1)
    print(a.value)
    print(a.get())
    print(a.name)
    print(a.sub)
