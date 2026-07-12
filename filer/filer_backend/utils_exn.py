from pydantic import BaseModel


class PydanticException(BaseModel):
    humanMessage: str = ''


class SerialException(Exception):

    def __init__(self, serialized: PydanticException, additional_message_prefix: str = ''):
        self._serialized = serialized
        super().__init__(f"{additional_message_prefix}{self._serialized.__class__.__name__}[{self._serialized}]")

    @property
    def serialized(self):
        return self._serialized
