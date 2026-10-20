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
    model_config = ConfigDict(extra='forbid', polymorphic_serialization=True)

    # __type_map is a class variable that maps module names to the classes in that module that implement this abstract model.
    __type_map: ClassVar[dict[str, dict[str, type[Self]]]]
    __base_model: ClassVar[type[Self] | None] = None


    def __init_subclass__(cls: type[Self], **kwargs):
        # Direct subclasses of AbstractBaseModel are an abstract type and receive a type_map.
        # Subclasses of those subclasses are implementations of that abstract type.
        # They register themselves in their parent's type_map.
        if cls.__base_model is None:
            cls.__type_map = {}
            cls.__base_model = cls
        else:
            module_name = cls.__module__.rsplit('.', 1)[-1]
            cls.__base_model.__type_map.setdefault(module_name, dict())[cls.__name__] = cls
        return super().__init_subclass__(**kwargs)

    def __new__(cls, **kwargs):
        # Instantiating the base model returns the appropriate subclass based on the "type" field.
        if cls is cls.__base_model:
            impl_cls = cls.get_class(kwargs.pop("type"))
            return impl_cls.__new__(impl_cls, **kwargs)
        
        if "type" in kwargs:
            raise ValueError(f"Unexpected 'type' field for {cls.__name__} instantiation.")
        return super().__new__(cls)

    def __init__(cls, **kwargs):
        # The type field is only needed when instantiating the base model.
        # It is only accessed from __new__ and can be removed here.
        kwargs.pop("type", None) 
        super().__init__(**kwargs)

    @classmethod
    def get_class(cls, type_name: str) -> type[Self]:
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

        if module_name not in cls.__type_map:
            raise ImportError(f"module '{repr(imported_module)}' does not contain any classes that implement '{cls.__name__}'")

        if len(cls.__type_map[module_name]) == 1:
            return next(iter(cls.__type_map[module_name].values()))
        elif class_name is None:
            raise ValueError(f"Multiple implementations of '{cls.__name__}' in '{module_name}'. Please specify the class name.")
        else:
            return cls.__type_map[module_name][class_name]    
    
    @model_serializer(mode='wrap')
    def serialize_model(
        self, handler: SerializerFunctionWrapHandler
    ) -> dict[str, object]:
        serialized = handler(self)
        module_name = self.__class__.__module__.rsplit('.', 1)[-1]
        serialized['type'] = f"{module_name}.{self.__class__.__name__}"
        return serialized
