from basetypes.implementation.exceptions.common_exceptions import ExpectedTypeException, HumanReadableException
from utils.custom_context_var import ContextVarWrapper

from contextlib import AbstractAsyncContextManager, _GeneratorContextManager
from typing import Type, Any, Callable, Iterable
from typing_extensions import AsyncIterable
from contextvars import ContextVar
import contextlib


class NotInAsyncContextManager(Exception):

    def __init__(self, method_name, class_name):
        super().__init__(f"`{method_name}` function of {class_name} intends to be executed within `async with [{class_name}_instance]`")


class AsyncContextManagerDependencyNotEntered(Exception):

    def __init__(self, context_var, expected_class, method_name):
        super().__init__(f"`{method_name}` function intends to be executed with a valid `{context_var}` async entered from [{expected_class} producer instance]`")


_reset_wrapping_context_managers = ContextVarWrapper('reset_wrapping_context_managers', default={})
_reset_wrapping_context_managers_sync = ContextVarWrapper('reset_wrapping_context_managers_sync', default={})

def register_manager_on_context_update(ctxt: ContextVar | ContextVarWrapper, ctxt_manager: AbstractAsyncContextManager):
    _reset_wrapping_context_managers.setdefault(ctxt, []).append(ctxt_manager)

def register_manager_on_context_update_sync(ctxt: ContextVar, sync_ctxt_manager: _GeneratorContextManager):
    _reset_wrapping_context_managers_sync.setdefault(ctxt, []).append(sync_ctxt_manager)


@contextlib.asynccontextmanager
async def _with_cascade_context_updates(modified_contextvar: ContextVar, instance):
    if modified_contextvar in _reset_wrapping_context_managers.get():
        async with contextlib.AsyncExitStack() as stack:
            _ = [stack.enter_context(async_manager(instance)) for async_manager in _reset_wrapping_context_managers.get()[modified_contextvar]]
            yield
    else:
        yield

@contextlib.contextmanager
def _with_cascade_context_updates_sync(modified_contextvar: ContextVar, instance):
    if modified_contextvar in _reset_wrapping_context_managers_sync.get():
        with contextlib.ExitStack() as stack:
            _ = [stack.enter_context(async_manager(instance)) for async_manager in _reset_wrapping_context_managers_sync.get()[modified_contextvar]]
            yield
    else:
        yield


_static_arguments_for_context = ContextVar('static_arguments', default={})
_dynamic_callable_arguments_for_context = ContextVar('dynamic_arguments', default={})

@contextlib.contextmanager
def bind_static(context_var: ContextVar | ContextVarWrapper, **kwargs):
    prev = _static_arguments_for_context.get().setdefault(context_var, {})
    try:
        _static_arguments_for_context.get()[context_var] = {**prev, **kwargs}
        yield
    finally:
        _static_arguments_for_context.get()[context_var] = prev

@contextlib.contextmanager
def bind_dynamic(context_var: ContextVar | ContextVarWrapper, **kwargs):
    prev = _dynamic_callable_arguments_for_context.get().setdefault(context_var, {})
    try:
        _dynamic_callable_arguments_for_context.get()[context_var] = {**prev, **kwargs}
        yield
    finally:
        _dynamic_callable_arguments_for_context.get()[context_var] = prev


def run_within(ModelType: Type | Callable, ctxt: ContextVar | ContextVarWrapper,
               default_bind_static_arguments: dict[str, Any] = None,
               default_bind_callable_arguments: dict[str, Callable] = None,
               upper_context_dependency: AbstractAsyncContextManager[dict[str, Any]] | None = None,
               with_static_bound_arguments: bool = True,
               with_dynamic_bound_arguments: bool = True,
               reenter_context: bool = False):

    if hasattr(ModelType, '__call__'):
        ModelTypeHint = Type
    else:
        ModelTypeHint = ModelType

    @contextlib.asynccontextmanager
    async def run_with_ctxt_manager(instance: ModelTypeHint | None = None, **kwargs) -> AsyncIterable[ModelTypeHint]:
        if instance and not isinstance(instance, ModelType):
            raise ExpectedTypeException(got=type(instance), expected=ModelType)
        if instance and kwargs:
            raise HumanReadableException('Expected no kwargs when giving already defined instance in run_within')
        additional = {}
        for k in default_bind_callable_arguments or {}:
            if k not in kwargs:
                additional[k] = default_bind_callable_arguments[k]()
        for k in default_bind_static_arguments or {}:
            if k not in kwargs and k not in additional:
                additional[k] = default_bind_static_arguments[k]
        if with_static_bound_arguments:
            sa = _static_arguments_for_context.get()
            if ctxt in sa:
                for k in sa[ctxt]:
                    additional[k] = sa[ctxt][k]
        if with_dynamic_bound_arguments:
            da = _dynamic_callable_arguments_for_context.get()
            if ctxt in da:
                for k in da[ctxt]:
                    additional[k] = da[ctxt][k]()

        @contextlib.asynccontextmanager
        async def finish(instance, reentered: bool = False):
            previous_instance = ctxt.set(instance)
            if reenter_context and not reentered:
                async with (
                    instance,
                    finish(instance, True)
                ):
                    yield
                return
            try:
                async with _with_cascade_context_updates(ctxt, instance):
                    yield
            finally:
                ctxt.reset(previous_instance)

        if upper_context_dependency:
            async with upper_context_dependency() as additional_from_upper_layer:
                instance = instance or ModelType(**additional, **kwargs, **additional_from_upper_layer)
                async with finish(instance):
                    yield instance
        else:
            instance = instance or ModelType(**additional, **kwargs)
            async with finish(instance):
                yield instance

    return run_with_ctxt_manager


def run_within_sync(ModelType: Type, ctxt: ContextVar | ContextVarWrapper,
                    default_bind_static_arguments: dict[str, Any] = None,
                    default_bind_callable_arguments: dict[str, Callable] = None,
                    upper_context_dependency: _GeneratorContextManager[dict[str, Any]] | None = None,
                    with_static_bound_arguments: bool = True,
                    with_dynamic_bound_arguments: bool = True):

    @contextlib.contextmanager
    def run_with_ctxt_manager(instance: ModelType | None = None, **kwargs) -> Iterable[ModelType]:
        if instance and not isinstance(instance, ModelType):
            raise ExpectedTypeException(got=type(instance), expected=ModelType)
        if instance and kwargs:
            raise HumanReadableException('Expected no kwargs when giving already defined instance in run_within')
        additional = {}
        for k in default_bind_callable_arguments or {}:
            if k not in kwargs:
                additional[k] = default_bind_callable_arguments[k]()
        for k in default_bind_static_arguments or {}:
            if k not in kwargs and k not in additional:
                additional[k] = default_bind_static_arguments[k]
        if with_static_bound_arguments:
            sa = _static_arguments_for_context.get()
            if ctxt in sa:
                for k in sa[ctxt]:
                    additional[k] = sa[ctxt][k]
        if with_dynamic_bound_arguments:
            da = _dynamic_callable_arguments_for_context.get()
            if ctxt in da:
                for k in da[ctxt]:
                    additional[k] = da[ctxt][k]()

        @contextlib.contextmanager
        def finish(instance):
            previous_instance = ctxt.set(instance)
            try:
                with _with_cascade_context_updates_sync(ctxt, instance):
                    yield
            finally:
                ctxt.reset(previous_instance)

        if upper_context_dependency:
             with upper_context_dependency() as additional_from_upper_layer:
                instance = instance or ModelType(**additional, **kwargs, **additional_from_upper_layer)
                with finish(instance):
                    yield instance
        else:
            instance = instance or ModelType(**additional, **kwargs)
            with finish(instance):
                yield instance

    return run_with_ctxt_manager


run_within_new_default_static_arguments = run_within(dict, _static_arguments_for_context)
run_within_new_default_dynamic_arguments = run_within(dict, _dynamic_callable_arguments_for_context)


if __name__ == '__main__':
    from anyio import run

    async def main():
        c1 = ContextVar[dict]('testcvar')
        t1 = run_within(dict, c1,
                        default_bind_static_arguments={'v1': 1, 'still': 'default val should be rewritten'},
                        default_bind_callable_arguments={'v2': lambda: '2'})
        with bind_static(c1, still2='stillthere'):
            with (
                bind_static(c1, still='stillthere2'),
                bind_dynamic(c1, still2=lambda: 8888)
            ):
                async with t1(nextarg1=6, nextarg2='other') as x:
                    print(x)
            async with t1(nextarg1=1337, nextarg2='other3') as x:
                print(x)

            async with t1('fail') as x:
                print(x)

    run(main)
