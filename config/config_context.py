from __future__ import annotations

from config.config_api import StructuredConfigApi
from config.config_from_enum import default_config_for_enum

from pydantic import BaseModel

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Type, Dict
from enum import Enum


class ConfigContext(StructuredConfigApi):

    def __init__(
        self,
        SubconfigEnum: Type[Enum] | None = None,
        BaseConfigAttributesType: Type[BaseModel] | None = None,
        *,
        with_default: bool = False,
        root: ConfigContext | None = None
    ):
        assert SubconfigEnum or BaseConfigAttributesType

        self._ConfigEnumType = SubconfigEnum
        self._BaseConfigAttributes = BaseConfigAttributesType
        self._default_additional_dict = BaseConfigAttributesType().model_dump() if with_default else {}
        self._context = ContextVar(self._BaseConfigAttributes and self._BaseConfigAttributes.__name__ or
                                   self._ConfigEnumType.__name__.replace('Type', ''))
        self._children_modified = False
        # children dict is append-only dict
        self._children: Dict[Enum, Type[BaseModel]] = {}
        self._config_template = None
        self._write_claimed = False
        self._root = root

    def _root_config_effect(self):
        # only trigger root config read if we are root
        if not self._root:
            pass

    def _update_contextvar_if_modified_children(self):
        if self._children_modified or not self._config_template:
            self._config_template = default_config_for_enum(self._children, self._BaseConfigAttributes)
            self._root_config_effect()
            self._context = ContextVar(self._BaseConfigAttributes.__name__, default=self._config_template()) \
                if with_default else ContextVar(self._config_template.__name__)
            self._children_modified = False

    def register_child(
        self,
        enum_val,
        *,
        SubconfigEnum: Type[Enum] | None = None,
        BaseConfigAttributesType: Type[BaseModel] | None = None,
        with_default: bool = False,
        with_default_instance: Type[BaseModel] | None = None
    ):
        assert isinstance(enum_val, self._ConfigEnumType)
        assert enum_val not in self._children

        child = ConfigContext(SubconfigEnum, BaseConfigAttributesType,
                              with_default=with_default, root=self._root or self)
        self._children_modified = True
        self._children = {
            **self._children,
            enum_val: child
        }
        return child

    @contextmanager
    def check_config(self):  # just get the current config object, if not set yet it will raise
        config_context = self._context.get()
        yield config_context

    @contextmanager
    def ensure_config(self):  # ensure the contextvar is fulfilled, create it if needed
        config_context = self._context.get(None)
        if not config_context:  # as config should not be None, we can avoid raising exception for enforcing config resolution
            config_context = self.resolve_config()
            self._context.set(config_context)
        yield config_context

    @contextmanager
    def replace_config(self, config):  # even if the config context is already set, replace it with provided argument
        pass

    @contextmanager
    def writeable_config(self):  # get the config as writeable, and write modifications at the end, + ensure unicity of write
        if self._write_claimed:
            raise

    def resolve_config(self):
        pass

    def parse_config(self):
        pass

    def config_example(self):
        pass
