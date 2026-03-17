from __future__ import annotations

from pydantic import BaseModel

from contextlib import contextmanager
from typing import Type, Protocol
from enum import Enum


class StructuredConfigApi(Protocol):

    """
    Configuration child registration API: must give either a Subconfig Enum (in case it's some non-leaf config node
    which references other)
    or a BaseConfigAttributes Type which must be an extension of some pydantic BaseModel, in this case this may be a
    config leaf with high chances (or it's additional attributes that are shared by all children)
    """
    def register_child(
        self,
        enum_val,
        *,
        SubconfigEnum: Type[Enum] | None = None,
        BaseConfigAttributesType: Type[BaseModel] | None = None,
        with_default: bool = False,
        with_default_instance: Type[BaseModel] | None = None
    ):
        ...

    """
    Should yield the contextvar holding subconfig; raises exception if contextvar does not exist
    """
    @contextmanager
    def check_config(self):
        ...

    """
    Should yield the contextvar holding subconfig; create and set the config context if not existing yet
    """
    @contextmanager
    def ensure_config(self):
        ...

    """
    Should replace and yield the contextvar holding subconfig, even if it is currently set
    """
    @contextmanager
    def replace_config(self, config):
        ...

    """
    Get the current contextvar holding subconfig as a writeable configuration object which will save back its state at
    the end of proper ending, within the persistence it comes from
    """
    @contextmanager
    def writeable_config(self):
        ...

    """
    Configuration resolution function
    """
    def resolve_config(self):
        ...

    """
    Configuration parsing function
    """
    def parse_config(self):
        ...

    """
    Configuration example function
    """
    def config_example(self):
        ...
