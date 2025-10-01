"""
Data models representing notification messages for the event bus

"""

from enum import Enum

from pydantic import BaseModel


class CallProcessStatus(str, Enum):
    CREATED = "created"
    DOWNLOADED = "downloaded"
    TRANSCRIBED = "transcribed"
    EVALUATED = "evaluated"


class Message(BaseModel):
    version: str


class RecordingProcessEvent(Message):
    version: str = "1.0"

    event_type: CallProcessStatus
    recording_id: int


class IncomingCall(Message):
    version: str = "1.0"

    event_type: CallProcessStatus
    event_id: int
