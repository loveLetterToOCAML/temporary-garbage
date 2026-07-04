from contextvars import ContextVar

from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String
import enum

from baseimplems.anyio_utils import run_within
from baseimplems.persistence.model_utils.date_formating_context import current_date


class NameCreationPolicy(enum.Enum):
    DATE_PREFIX = enum.auto()
    INDEX_PREFIX = enum.auto()
    DATE_SUFFIX = enum.auto()
    INDEX_SUFFIX = enum.auto()
    COMPLEX_PREFIX = enum.auto()  # what, who, when (should be in current context)


name_creation_policy = ContextVar[NameCreationPolicy]('name_creation_policy', default=NameCreationPolicy.DATE_SUFFIX)
run_with_naming_policy = run_within(NameCreationPolicy, name_creation_policy)


def prefix_policy(classname, index, name, force_index: bool = False):
    ncp = name_creation_policy.get()
    if ncp is NameCreationPolicy.DATE_PREFIX:
        return current_date() + '-' + (f"{str(index).zfill(3)}-" if force_index else '')
    elif ncp is NameCreationPolicy.INDEX_PREFIX:
        return str(index).zfill(3) + '-'
    elif ncp is NameCreationPolicy.COMPLEX_PREFIX:
        what = classname.name_prefix() + '.' if hasattr(classname, 'name_prefix') else ''
        return f"{what}{current_date()}" \
               f".{ctxt.get('interactor', {}).get('who', '?')}." + (f"{str(index).zfill(3)}-" if force_index else '')
    else:
        return name

def suffix_policy(classname, index, name, force_index: bool = False):
    ncp = name_creation_policy.get()
    if ncp is NameCreationPolicy.DATE_SUFFIX:
        return '-' + current_date() + (f"-{str(index).zfill(3)}" if force_index else '')
    elif ncp is NameCreationPolicy.INDEX_SUFFIX:
        return '-' + str(index).zfill(3)
    else:
        return name


if __name__ == '__main__':
    import anyio

    async def main():
        async with run_with_naming_policy(NameCreationPolicy.DATE_SUFFIX):
            print(prefix_policy(str,1, 'test1', True))
            print(suffix_policy(str,1, 'test2', False))
        async with run_with_naming_policy(NameCreationPolicy.INDEX_PREFIX):
            print(prefix_policy(str,1, 'test3', False))
            print(suffix_policy(str,1, 'test4', True))

    anyio.run(main)
