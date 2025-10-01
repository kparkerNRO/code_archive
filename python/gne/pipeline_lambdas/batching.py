import json
from typing import Annotated, Any

from common.messaging.events import EventBridgeMessage
from common.messaging.models import Message
from pydantic import AliasGenerator, BaseModel, ConfigDict, SerializationInfo, WrapSerializer
from pydantic.alias_generators import to_camel


def list_output(value: list[Message], handler, info: SerializationInfo) -> int | dict[str, Any]:
    """
    Helper method to output only counts by default, but support an optional "detailed" context
    to output the messages as well
    """
    context = info.context
    if not context:
        return len(value)

    detailed = context.get("detailed", False)
    if not detailed:
        return len(value)

    return {"count": len(value), "messages": value}


class BatchItemStats(BaseModel):
    model_config = ConfigDict(alias_generator=AliasGenerator(serialization_alias=to_camel))

    total: int
    succeeded: Annotated[list[Message], WrapSerializer(list_output)]
    failed: Annotated[list[Message], WrapSerializer(list_output)]
    unprocessed: Annotated[list[Message], WrapSerializer(list_output)]


class BatchOutput(BaseModel):
    model_config = ConfigDict(alias_generator=AliasGenerator(serialization_alias=to_camel))

    status_code: int
    batch_item_failures: list[dict[str, str]] | None = None
    stats: BatchItemStats


class BatchTracker:
    def __init__(self, event, event_type):
        if "Records" in event:
            # For SQS events, store both the parsed EventBridge event and the original SQS record
            self.event_record_pairs = []
            self.event_sqs_record_pairs = []  # Store mapping to original SQS records
            for sqs_record in event["Records"]:
                parsed_event = json.loads(sqs_record["body"])
                event_bridge_msg = EventBridgeMessage.from_bridge_event(parsed_event, event_type)

                self.event_record_pairs.append((event_bridge_msg, parsed_event))
                self.event_sqs_record_pairs.append((event_bridge_msg, sqs_record))

        elif "detail" in event:
            # For direct EventBridge events, store the event itself
            event_bridge_msg = EventBridgeMessage.from_bridge_event(event, event_type)
            self.event_record_pairs = [(event_bridge_msg, event)]
            self.event_sqs_record_pairs = []  # No SQS records for direct events
        else:
            raise ValueError("Expected 'detail' or 'Records' field in event")

        self.events = [pair[0] for pair in self.event_record_pairs]
        self.succeeded_events: list[EventBridgeMessage] = []
        self.failed_events: list[EventBridgeMessage] = []
        self.processed_events: list[EventBridgeMessage] = []

    def mark_event(self, event: EventBridgeMessage, is_successful: bool = True):
        if is_successful:
            self.succeeded_events.append(event)
        else:
            self.failed_events.append(event)
        self.processed_events.append(event)

    def is_event_succeeded(self, event: EventBridgeMessage):
        return event in self.succeeded_events

    def get_record_for_event(self, event: EventBridgeMessage):
        """Get the original record for a given EventBridgeMessage."""
        for event_bridge_msg, record in self.event_record_pairs:
            if event_bridge_msg is event:
                return record
        raise ValueError("Event not found in tracked events")

    def get_message_id_for_event(self, event: EventBridgeMessage):
        """Get the SQS messageId for a given EventBridgeMessage, if it exists."""
        for event_bridge_msg, sqs_record in self.event_sqs_record_pairs:
            if event_bridge_msg is event:
                return sqs_record["messageId"]
        return None  # No messageId for direct EventBridge events

    def generate_response(self, detailed_logging: bool = False):
        """
        Generate a response output for the state of the tracker
        """
        unprocessed_events = [
            event
            for event in self.processed_events
            if (event not in self.succeeded_events and event not in self.failed_events)
        ]

        failed_ids = [event.detail for event in self.failed_events]
        unprocessed_ids = [event.detail for event in unprocessed_events]
        succeeded_ids = [event.detail for event in self.succeeded_events]
        batch_item_failures = []

        if self.event_sqs_record_pairs != []:
            # Only include batchItemFailures for SQS events (which have messageId)
            failed_message_ids = [self.get_message_id_for_event(event) for event in self.failed_events]
            unprocessed_message_ids = [self.get_message_id_for_event(event) for event in unprocessed_events]
            all_failed_ids = failed_message_ids + unprocessed_message_ids
            if all_failed_ids:
                batch_item_failures = all_failed_ids

        stats = BatchItemStats(
            total=len(self.events),
            succeeded=succeeded_ids,
            failed=failed_ids,
            unprocessed=unprocessed_ids,
        )

        num_failed = len(self.failed_events) + len(unprocessed_events)
        if num_failed == len(self.events):
            status_code = 500
        elif num_failed > 0:
            status_code = 207
        else:
            status_code = 200

        batch_output = BatchOutput(
            status_code=status_code,
            batch_item_failures=[{"itemIdentifier": failed_message_id} for failed_message_id in batch_item_failures],
            stats=stats,
        )

        context = {"detailed": detailed_logging}
        return batch_output.model_dump(by_alias=True, context=context, exclude_none=True)
