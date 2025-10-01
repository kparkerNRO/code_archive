import json
from unittest.mock import Mock

import pytest
from common.messaging.events import EventBridgeMessage
from common.messaging.models import CallProcessStatus, IncomingCall, RecordingProcessEvent

from data_pipeline.shared.batching import BatchItemStats, BatchTracker


class TestBatchItemStats:
    """Test suite for the BatchItemStats class."""

    @pytest.fixture
    def sample_batch_stats(self):
        """Create a sample BatchItemStats for testing."""
        return BatchItemStats(
            total=10,
            succeeded=[
                RecordingProcessEvent(event_type=CallProcessStatus.TRANSCRIBED, recording_id=1),
                RecordingProcessEvent(event_type=CallProcessStatus.TRANSCRIBED, recording_id=2),
                RecordingProcessEvent(event_type=CallProcessStatus.TRANSCRIBED, recording_id=3),
            ],
            failed=[
                RecordingProcessEvent(event_type=CallProcessStatus.DOWNLOADED, recording_id=4),
                RecordingProcessEvent(event_type=CallProcessStatus.DOWNLOADED, recording_id=5),
            ],
            unprocessed=[
                RecordingProcessEvent(event_type=CallProcessStatus.EVALUATED, recording_id=6),
                RecordingProcessEvent(event_type=CallProcessStatus.EVALUATED, recording_id=7),
                RecordingProcessEvent(event_type=CallProcessStatus.EVALUATED, recording_id=8),
            ],
        )

    def test_serialization_no_context(self, sample_batch_stats):
        """Test serialization without context returns length only."""
        # Serialize without context
        result = sample_batch_stats.model_dump()

        assert result["total"] == 10
        assert result["succeeded"] == 3  # Length of succeeded list
        assert result["failed"] == 2  # Length of failed list
        assert result["unprocessed"] == 3  # Length of unprocessed list

    def test_serialization_context_detailed_false(self, sample_batch_stats):
        """Test serialization with detailed=False returns length only."""
        # Serialize with detailed=False context
        result = sample_batch_stats.model_dump(context={"detailed": False})

        assert result["total"] == 10
        assert result["succeeded"] == 3  # Length of succeeded list
        assert result["failed"] == 2  # Length of failed list
        assert result["unprocessed"] == 3  # Length of unprocessed list

    def test_serialization_context_detailed_true(self, sample_batch_stats):
        """Test serialization with detailed=True returns count and ids."""
        # Serialize with detailed=True context
        result = sample_batch_stats.model_dump(context={"detailed": True})

        assert result["total"] == 10

        # Check succeeded field structure
        assert result["succeeded"]["count"] == 3
        assert len(result["succeeded"]["messages"]) == 3
        # Check the actual structure of the message serialization
        first_message = result["succeeded"]["messages"][0]
        assert first_message["event_type"] == "transcribed"
        assert first_message["recording_id"] == 1
        assert result["succeeded"]["messages"][1]["recording_id"] == 2
        assert result["succeeded"]["messages"][2]["recording_id"] == 3

        # Check failed field structure
        assert result["failed"]["count"] == 2
        assert len(result["failed"]["messages"]) == 2
        assert result["failed"]["messages"][0]["event_type"] == "downloaded"
        assert result["failed"]["messages"][0]["recording_id"] == 4
        assert result["failed"]["messages"][1]["recording_id"] == 5

        # Check unprocessed field structure
        assert result["unprocessed"]["count"] == 3
        assert len(result["unprocessed"]["messages"]) == 3
        assert result["unprocessed"]["messages"][0]["event_type"] == "evaluated"
        assert result["unprocessed"]["messages"][0]["recording_id"] == 6
        assert result["unprocessed"]["messages"][1]["recording_id"] == 7
        assert result["unprocessed"]["messages"][2]["recording_id"] == 8

    def test_serialization_empty_lists(self):
        """Test serialization with empty lists."""
        stats = BatchItemStats(
            total=0,
            succeeded=[],
            failed=[],
            unprocessed=[],
        )

        # Test without context
        result = stats.model_dump()
        assert result["succeeded"] == 0
        assert result["failed"] == 0
        assert result["unprocessed"] == 0

        # Test with detailed=True context
        result = stats.model_dump(context={"detailed": True})
        assert result["succeeded"]["count"] == 0
        assert result["succeeded"]["messages"] == []
        assert result["failed"]["count"] == 0
        assert result["failed"]["messages"] == []
        assert result["unprocessed"]["count"] == 0
        assert result["unprocessed"]["messages"] == []

    def test_serialization_context_other_values(self, sample_batch_stats):
        """Test serialization with context containing other values but no detailed key."""
        # Serialize with context that doesn't contain 'detailed' key
        result = sample_batch_stats.model_dump(context={"other_key": "other_value"})

        assert result["total"] == 10
        assert result["succeeded"] == 3  # Should default to length
        assert result["failed"] == 2
        assert result["unprocessed"] == 3


class TestBatchTracker:
    """Test suite for the BatchTracker class."""

    @pytest.fixture
    def mock_event_bridge_message(self):
        """Create a mock EventBridgeMessage for testing."""
        mock_detail = IncomingCall(event_type=CallProcessStatus.CREATED, event_id=1)
        return EventBridgeMessage(source="test-source", detail=mock_detail, event_bus_name="test-bus")

    @pytest.fixture
    def mock_sqs_record(self):
        """Create a mock SQS record structure."""
        return {
            "messageId": "msg-123",
            "body": json.dumps(
                {
                    "source": "test-source",
                    "detail-type": "IncomingCall",
                    "detail": {"version": "1.0", "event_type": "created", "event_id": 1},
                    "event_bus_name": "test-bus",
                    "messageId": "msg-123",
                }
            ),
        }

    @pytest.fixture
    def mock_sqs_event(self, mock_sqs_record):
        """Create a mock SQS event with Records."""
        return {"Records": [mock_sqs_record]}

    @pytest.fixture
    def mock_direct_event(self):
        """Create a mock direct EventBridge event."""
        return {
            "source": "test-source",
            "detail-type": "IncomingCall",
            "detail": {"version": "1.0", "event_type": "created", "event_id": 1},
            "event_bus_name": "test-bus",
        }

    def test_init_with_sqs_records(self, mock_sqs_event):
        """Test BatchTracker initialization with SQS Records."""
        tracker = BatchTracker(mock_sqs_event, IncomingCall)

        assert len(tracker.event_record_pairs) == 1
        assert len(tracker.events) == 1
        assert isinstance(tracker.events[0], EventBridgeMessage)
        assert tracker.events[0].source == "test-source"
        assert tracker.events[0].detail.event_type == CallProcessStatus.CREATED
        assert tracker.events[0].detail.event_id == 1
        assert tracker.succeeded_events == []
        assert tracker.failed_events == []
        assert tracker.processed_events == []

    def test_init_with_direct_event(self, mock_direct_event):
        """Test BatchTracker initialization with direct EventBridge event."""
        tracker = BatchTracker(mock_direct_event, IncomingCall)

        assert len(tracker.event_record_pairs) == 1
        assert len(tracker.events) == 1
        assert isinstance(tracker.events[0], EventBridgeMessage)
        assert tracker.events[0].source == "test-source"
        assert tracker.events[0].detail.event_type == CallProcessStatus.CREATED
        assert tracker.events[0].detail.event_id == 1
        assert tracker.succeeded_events == []
        assert tracker.failed_events == []
        assert tracker.processed_events == []

    def test_init_with_invalid_event(self):
        """Test BatchTracker initialization with invalid event structure."""
        invalid_event = {"invalid": "structure"}

        with pytest.raises(ValueError, match="Expected 'detail' or 'Records' field in event"):
            BatchTracker(invalid_event, IncomingCall)

    def test_mark_event_successful(self, mock_sqs_event):
        """Test marking an event as successful."""
        tracker = BatchTracker(mock_sqs_event, IncomingCall)
        event = tracker.events[0]  # Get the actual created event
        tracker.mark_event(event, is_successful=True)

        assert event in tracker.succeeded_events
        assert event not in tracker.failed_events
        assert event in tracker.processed_events

    def test_mark_event_failed(self, mock_sqs_event):
        """Test marking an event as failed."""
        tracker = BatchTracker(mock_sqs_event, IncomingCall)
        event = tracker.events[0]  # Get the actual created event
        tracker.mark_event(event, is_successful=False)

        assert event not in tracker.succeeded_events
        assert event in tracker.failed_events
        assert event in tracker.processed_events

    def test_get_record_for_event(self, mock_sqs_event):
        """Test retrieving the original record for an event."""
        tracker = BatchTracker(mock_sqs_event, IncomingCall)
        event = tracker.events[0]  # Get the actual created event
        record = tracker.get_record_for_event(event)

        expected_record = json.loads(mock_sqs_event["Records"][0]["body"])
        assert record == expected_record

    def test_get_record_for_event_not_found(self, mock_sqs_event):
        """Test retrieving record for an event that doesn't exist."""
        tracker = BatchTracker(mock_sqs_event, IncomingCall)

        # Try to get record for a different event
        different_event = Mock(spec=EventBridgeMessage)

        with pytest.raises(ValueError, match="Event not found in tracked events"):
            tracker.get_record_for_event(different_event)

    def test_generate_response_all_successful(self, mock_sqs_event):
        """Test generating response when all events are successful."""
        tracker = BatchTracker(mock_sqs_event, IncomingCall)
        event = tracker.events[0]  # Get the actual created event
        tracker.mark_event(event, is_successful=True)

        response = tracker.generate_response()

        assert response["statusCode"] == 200
        assert response["batchItemFailures"] == []
        assert response["stats"]["total"] == 1
        assert response["stats"]["failed"] == 0
        assert response["stats"]["succeeded"] == 1
        assert response["stats"]["unprocessed"] == 0

    def test_generate_response_with_failures(self, mock_sqs_event, mock_sqs_record):
        """Test generating response when some events fail."""
        tracker = BatchTracker(mock_sqs_event, IncomingCall)
        event = tracker.events[0]  # Get the actual created event
        tracker.mark_event(event, is_successful=False)

        response = tracker.generate_response()

        assert response["statusCode"] == 500
        assert "batchItemFailures" in response
        assert {"itemIdentifier": mock_sqs_record["messageId"]} in response["batchItemFailures"]
        assert response["stats"]["total"] == 1
        assert response["stats"]["failed"] == 1
        assert response["stats"]["succeeded"] == 0
        assert response["stats"]["unprocessed"] == 0

    def test_generate_response_with_unprocessed(self, mock_sqs_event, mock_sqs_record):
        """Test generating response with unprocessed events."""
        tracker = BatchTracker(mock_sqs_event, IncomingCall)
        event = tracker.events[0]  # Get the actual created event
        # Add to processed but not to succeeded or failed
        tracker.processed_events.append(event)

        response = tracker.generate_response()

        assert response["statusCode"] == 500
        assert "batchItemFailures" in response
        assert {"itemIdentifier": mock_sqs_record["messageId"]} in response["batchItemFailures"]
        assert response["stats"]["total"] == 1
        assert response["stats"]["failed"] == 0
        assert response["stats"]["succeeded"] == 0
        assert response["stats"]["unprocessed"] == 1

    def test_generate_response_all_failed(self, mock_direct_event):
        """Test generating response when all events fail."""
        tracker = BatchTracker(mock_direct_event, IncomingCall)
        event = tracker.events[0]  # Get the actual created event
        tracker.mark_event(event, is_successful=False)

        response = tracker.generate_response()

        assert response["statusCode"] == 500

    def test_generate_response_direct_event_no_message_id(self, mock_direct_event):
        """Test generating response with direct EventBridge event (no messageId)."""
        tracker = BatchTracker(mock_direct_event, IncomingCall)
        event = tracker.events[0]  # Get the actual created event
        tracker.mark_event(event, is_successful=False)

        response = tracker.generate_response()

        # For direct events, there should be an empty batchItemFailures since there's no messageId
        assert response["statusCode"] == 500
        assert response["batchItemFailures"] == []
        assert response["stats"]["total"] == 1
        assert response["stats"]["failed"] == 1
        assert response["stats"]["succeeded"] == 0
        assert response["stats"]["unprocessed"] == 0

    def test_generate_response_detailed_logging(self, mock_sqs_event, mock_sqs_record):
        """Test generating response with detailed logging enabled."""
        tracker = BatchTracker(mock_sqs_event, IncomingCall)
        event = tracker.events[0]  # Get the actual created event
        tracker.mark_event(event, is_successful=False)

        response = tracker.generate_response(detailed_logging=True)

        assert response["statusCode"] == 500
        assert "stats" in response
        assert "count" in response["stats"]["failed"]
        assert "messages" in response["stats"]["failed"]
        assert "count" in response["stats"]["unprocessed"]
        assert "messages" in response["stats"]["unprocessed"]
        assert response["stats"]["failed"]["count"] == 1
        assert response["stats"]["unprocessed"]["count"] == 0

    def test_generate_response_multiple_records(self):
        """Test generating response with multiple SQS records."""
        # Create multiple SQS records
        records = []
        for i in range(3):
            records.append(
                {
                    "messageId": f"msg-{i}",
                    "body": json.dumps(
                        {
                            "source": "test-source",
                            "detail-type": "IncomingCall",
                            "detail": {"version": "1.0", "event_type": "created", "event_id": i},
                            "event_bus_name": "test-bus",
                        }
                    ),
                }
            )

        multi_record_event = {"Records": records}

        tracker = BatchTracker(multi_record_event, IncomingCall)

        # Mark first event as successful, second as failed, leave third unprocessed
        tracker.mark_event(tracker.events[0], is_successful=True)
        tracker.mark_event(tracker.events[1], is_successful=False)
        tracker.processed_events.append(tracker.events[2])  # Unprocessed

        response = tracker.generate_response()

        assert response["statusCode"] == 207
        assert response["stats"]["total"] == 3
        assert response["stats"]["succeeded"] == 1
        assert response["stats"]["failed"] == 1
        assert response["stats"]["unprocessed"] == 1
        assert len(response["batchItemFailures"]) == 2  # Failed + unprocessed

    def test_generate_response_no_result_code_success(self, mock_sqs_event):
        """Test generating response with no result code set and successful processing."""
        tracker = BatchTracker(mock_sqs_event, IncomingCall)
        event = tracker.events[0]  # Get the actual created event
        tracker.mark_event(event, is_successful=True)
        tracker.result_code = None

        response = tracker.generate_response()

        assert response["statusCode"] == 200

    def test_multiple_sqs_records_json_parsing(self):
        """Test that JSON parsing works correctly for multiple SQS records."""
        records = [
            {
                "messageId": "msg-1",
                "body": json.dumps(
                    {
                        "source": "test-source-1",
                        "detail-type": "IncomingCall",
                        "detail": {"version": "1.0", "event_type": "created", "event_id": 1},
                        "event_bus_name": "test-bus-1",
                    }
                ),
            },
            {
                "messageId": "msg-2",
                "body": json.dumps(
                    {
                        "source": "test-source-2",
                        "detail-type": "IncomingCall",
                        "detail": {"version": "1.0", "event_type": "created", "event_id": 2},
                        "event_bus_name": "test-bus-2",
                    }
                ),
            },
        ]

        event = {"Records": records}

        tracker = BatchTracker(event, IncomingCall)

        assert len(tracker.events) == 2
        assert len(tracker.event_record_pairs) == 2

        # Verify that the events were created correctly
        assert tracker.events[0].source == "test-source-1"
        assert tracker.events[0].detail.event_id == 1
        assert tracker.events[0].event_bus_name == "test-bus-1"

        assert tracker.events[1].source == "test-source-2"
        assert tracker.events[1].detail.event_id == 2
        assert tracker.events[1].event_bus_name == "test-bus-2"
