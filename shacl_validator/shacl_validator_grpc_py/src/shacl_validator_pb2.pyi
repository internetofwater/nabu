from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class JsoldValidationRequest(_message.Message):
    __slots__ = ("jsonld",)
    JSONLD_FIELD_NUMBER: _ClassVar[int]
    jsonld: str
    def __init__(self, jsonld: _Optional[str] = ...) -> None: ...

class ValidationReply(_message.Message):
    __slots__ = ("valid", "message")
    VALID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    valid: bool
    message: str
    def __init__(self, valid: bool = ..., message: _Optional[str] = ...) -> None: ...
