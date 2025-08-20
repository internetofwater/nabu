from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MatchingShaclType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LocationOriented: _ClassVar[MatchingShaclType]
    DatasetOriented: _ClassVar[MatchingShaclType]
LocationOriented: MatchingShaclType
DatasetOriented: MatchingShaclType

class JsoldValidationRequest(_message.Message):
    __slots__ = ("jsonld",)
    JSONLD_FIELD_NUMBER: _ClassVar[int]
    jsonld: str
    def __init__(self, jsonld: _Optional[str] = ...) -> None: ...

class ValidationReply(_message.Message):
    __slots__ = ("valid", "message", "ShaclType")
    VALID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    SHACLTYPE_FIELD_NUMBER: _ClassVar[int]
    valid: bool
    message: str
    ShaclType: MatchingShaclType
    def __init__(self, valid: bool = ..., message: _Optional[str] = ..., ShaclType: _Optional[_Union[MatchingShaclType, str]] = ...) -> None: ...
