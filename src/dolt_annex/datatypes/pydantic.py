#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import ABC
import importlib
from typing_extensions import Any, ClassVar, Optional, Self

from pydantic import ConfigDict, ModelWrapValidatorHandler, SerializerFunctionWrapHandler, model_serializer, model_validator
import pydantic

class StrictBaseModel(pydantic.BaseModel):
    """A pydantic base model that disallows extra fields."""
    model_config = ConfigDict(extra='forbid')
    
class AbstractBaseModel(StrictBaseModel, ABC):

    # _type_map is a class variable that maps module names to the classes in that module that implement this abstract model.
    _type_map: ClassVar[dict[str, dict[str, type[Self]]]]

    def __init_subclass__(cls: type[Self], **kwargs):
        cls._init()
        return super().__init_subclass__(**kwargs)

    @classmethod
    def _init(cls: type[Self]) -> None:
        cls._type_map = {}
        @classmethod
        def child_init_subclass__(child_cls: type[Self]):
            module_name = child_cls.__module__.rsplit('.', 1)[-1]
            cls._type_map.setdefault(module_name, dict())[child_cls.__name__] = child_cls
        cls._init = child_init_subclass__

    @classmethod
    def is_abstract(cls: type[Self]) -> bool:
        return bool(getattr(cls, "__abstractmethods__", False))

    @model_validator(mode='wrap')
    @classmethod
    def validator(cls, v: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:

        if isinstance(v, cls):
            return handler(v)

        type_name = v.pop("type", None)
        if type_name is None:
             # Concrete implementations use the normal handler
            return handler(v)

        class_name: Optional[str] = None
        match type_name.split('.'):
            case [module_name]:
                pass
            case [module_name, class_name]:
                pass
            case _:
                raise ImportError(f"Invalid type name '{type_name}'. Expected format 'ClassName' or 'ModuleName.ClassName'.")

        # Ensure that the module is imported. We don't need to use the return value of import_module here,
        # because the subclass will register itself in __init_subclass__
        imported_module = importlib.import_module(f"..{module_name}", package=cls.__module__)

        if module_name not in cls._type_map:
            raise ImportError(f"module '{repr(imported_module)}' does not contain any classes that implement '{cls.__name__}'")

        if len(cls._type_map[module_name]) == 1:
            impl_cls = next(iter(cls._type_map[module_name].values()))
        elif class_name is None:
            raise ValueError(f"Multiple implementations of '{cls.__name__}' in '{module_name}'. Please specify the class name.")
        else:
            impl_cls = cls._type_map[module_name][class_name]    
        
        return impl_cls(**v)
    
    @model_serializer(mode='wrap')
    def serialize_model(
        self, handler: SerializerFunctionWrapHandler
    ) -> dict[str, object]:
        serialized = handler(self)
        module_name = self.__class__.__module__.rsplit('.', 1)[-1]
        serialized['type'] = f"{module_name}.{self.__class__.__name__}"
        return serialized