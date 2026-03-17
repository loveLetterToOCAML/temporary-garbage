from utils.custom_context_var import ContextVarWrapper

from contextvars import ContextVar
from typing import Type
import contextlib


# set it as contextvar so that each threads has it own reset context
# TODO: check if the feature is still working in multithread context since registration may happen at one location only
_reset_wrapping_context_managers = ContextVarWrapper('reset_wrapping_context_managers', default={})


def run_with_policy(PolicyReadOnlyModel: Type, ctxt: ContextVar):
    @contextlib.contextmanager
    def run_with_policy_ctxt_manager(policy: PolicyReadOnlyModel | None = None, **kwargs) -> PolicyReadOnlyModel:
        policy = policy or PolicyReadOnlyModel(**kwargs)
        previous_policy = ctxt.set(policy)

        try:
            if ctxt in _reset_wrapping_context_managers.get():
                with contextlib.ExitStack() as stack:
                    _ = [stack.enter_context(o(policy)) for o in _reset_wrapping_context_managers.get()[ctxt]]
                    yield policy
            else:
                yield policy
        finally:
            ctxt.reset(previous_policy)

    return run_with_policy_ctxt_manager


def register_manager_on_context_update(ctxt: ContextVar, ctxt_manager):
    _reset_wrapping_context_managers.setdefault(ctxt, []).append(ctxt_manager)
