"""
Functions for handling event-bridge formatted events
"""

from typing import Annotated

from pydantic import BaseModel
from pydantic.types import StringConstraints

from .models import Message

SourceName = Annotated[str, StringConstraints(pattern=r"^\S+$")]


class EventException(Exception):
    pass


class EventBridgeMessage(BaseModel):
    source: SourceName
    detail: Message
    event_bus_name: str | None  # this isn't included in what's sent back to us, so it's allowed to be null

    def to_bridge_event(self) -> dict:
        return {
            "Source": self.source,
            "DetailType": self.detail.__class__.__name__,
            "Detail": self.detail.model_dump_json(),
            "EventBusName": self.event_bus_name,
        }

    def to_lambda_event(self) -> dict:
        return {
            "source": self.source,
            "detail-type": self.detail.__class__.__name__,
            "detail": self.detail.model_dump_json(),
            "event_bus_name": self.event_bus_name,
        }

    @staticmethod
    def from_bridge_event(bridge_event: dict[str, str], model_class: type[Message]):
        """
        Handles some of the different ways the bridge event can be parsed by the time
        it gets to the lambda event.

        This parsing is subject to change based on the actual AWS implementation,
        but it works against Moto
        """

        standardized_key_names = {key.lower(): value for key, value in bridge_event.items()}
        source_name = standardized_key_names["source"]

        class_name = standardized_key_names.get("detail-type", standardized_key_names.get("detailtype"))
        if model_class.__name__ != class_name:
            raise EventException("Detail type does not match expected Model.")

        raw_detail = standardized_key_names["detail"]
        if isinstance(raw_detail, str):
            detail = model_class.model_validate_json(raw_detail)
        else:
            detail = model_class.model_validate(raw_detail)

        event_bus = standardized_key_names.get("event_bus_name", standardized_key_names.get("eventbusname"))

        return EventBridgeMessage(source=source_name, detail=detail, event_bus_name=event_bus)
