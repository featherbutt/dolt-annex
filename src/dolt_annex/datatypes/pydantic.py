#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import ABC
import importlib
from typing import Any, ClassVar, Self
from pydantic import ModelWrapValidatorHandler, BaseModel, SerializerFunctionWrapHandler, model_serializer, model_validator

class AbstractBaseModel(BaseModel, ABC):

    _type_map: ClassVar[dict[str, type[Self]]]

    def __init_subclass__(cls: type[Self], **kwargs):
        cls._init()

    @classmethod
    def _init(cls: type[Self]) -> None:
        cls._type_map = {}
        @classmethod
        def child_init_subclass__(child_cls: type[Self]):
            cls._type_map[child_cls.__name__] = child_cls
        cls._init = child_init_subclass__

    @classmethod
    def is_abstract(cls: type[Self]) -> bool:
        return bool(getattr(cls, "__abstractmethods__", False))

    @model_validator(mode='wrap')
    @classmethod
    def validator(cls, v: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:

        if isinstance(v, cls):
            return handler(v)
        
        # Concrete implementations use the normal handler
        if not cls.is_abstract():
            return handler(v)
        
        type_name = v.pop("type", None)
        if type_name is None:
            raise ValueError("Missing 'type' field in configuration")

        class_name: str
        match type_name.split('.'):
            case [module_name]:
                class_name = module_name
            case [module_name, class_name]:
                pass
            case _:
                raise ImportError(f"Invalid type name '{type_name}'. Expected format 'ClassName' or 'ModuleName.ClassName'.")

        if class_name in cls._type_map:
            return cls._type_map[class_name](**v)

        # Ensure that the module is imported. We don't need to use the return value of import_module here,
        # because the subclass will register itself in __init_subclass__
        imported_module = importlib.import_module(f"..{module_name.lower()}", package=cls.__module__)

        if class_name not in cls._type_map:
            raise ImportError(f"'{imported_module}.{class_name}' does not exist or does not implement '{cls.__name__}'")

        impl_cls = cls._type_map[class_name]
        return impl_cls(**v)
    
    @model_serializer(mode='wrap')
    def serialize_model(
        self, handler: SerializerFunctionWrapHandler
    ) -> dict[str, object]:
        serialized = handler(self)
        serialized['type'] = self.__class__.__name__
        return serialized