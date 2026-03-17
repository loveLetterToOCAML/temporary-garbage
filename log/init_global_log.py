from context.config.init_config import InitialConfigContext

from context.config import current_config
from context.log.logging_context import run_with_log_policy
from policy.log import LogPolicy


def initialize_logging() -> LogPolicy:
    config = current_config.get()
    return config.LogPolicy


GlobalLoggingContext = register_dependent_init(
    'global_logging_context', initialize_logging, run_with_log_policy,
    depends_on=[InitialConfigContext]
)
