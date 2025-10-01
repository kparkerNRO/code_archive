import json
from dataclasses import dataclass
from typing import Any
from unittest.mock import patch

import pytest
import requests_mock
from common.database.database_fixtures import CallCenterUserFactory, CallFactory, RecordingFactory
from common.database.models import Call, CallCenterUser, CallProcess, Recording
from common.messaging.events import EventBridgeMessage
from common.messaging.models import CallProcessStatus, IncomingCall, RecordingProcessEvent
from common.models.db_enum import ProcessStatus
from s3pathlib import S3Path
from sqlalchemy import select
from sqlalchemy.orm import Session

from data_pipeline.conftest import SQSEventBridgeListener
from data_pipeline.shared.batching import BatchTracker
from data_pipeline.shared.errors import PipelineProcessingException
from data_pipeline.shared.talkdesk import TalkdeskSecret, get_bearer_token
from data_pipeline.talkdesk_download import (
    TalkdeskDownloadSettings,
    extract_filename,
    handler,
    run_all,
    run_thread,
    upload_media_to_s3,
)

EVENT_IDENTIFIER = "data-pipeline.talkdesk-download"


def create_test_event(detail):
    """Helper to create test EventBridge events."""
    return EventBridgeMessage(source=EVENT_IDENTIFIER, detail=detail, event_bus_name="test-event-bus").to_lambda_event()


def create_test_tracker(event_ids: list[int]) -> BatchTracker:
    """Helper to create BatchTracker for testing."""
    # Create a test event with multiple call IDs as separate SQS records
    sqs_records = []
    for event_id in event_ids:
        incoming_call_detail = IncomingCall(event_type=CallProcessStatus.CREATED, event_id=event_id)
        event = create_test_event(incoming_call_detail)
        sqs_record = {
            "messageId": f"msg-{event_id}",
            "body": json.dumps(event),
        }
        sqs_records.append(sqs_record)

    sqs_event = {"Records": sqs_records}
    return BatchTracker(sqs_event, IncomingCall)


@dataclass
class TalkDeskDownloadFixture:
    session: Session
    event_listener: SQSEventBridgeListener
    users: list[CallCenterUser]
    calls: list[Call]
    settings: TalkdeskDownloadSettings
    events_client: Any
    event_ids: list[int]
    call_ids: list[str]
    user: CallCenterUser
    call_ctr_path: str


@pytest.fixture()
def talkdesk_download_fixture(
    talkdesk_lambda_env,
    base_session,
    events_client,
    db_secret,
    talkdesk_secret,
    test_events_client,
    sqs_listener: SQSEventBridgeListener,
    test_event_bus,
    call_center_setup,
    encryption_secret,
):
    user = call_center_setup.agent

    call_center = call_center_setup.call_center
    users = [
        user,
        CallCenterUserFactory.create(call_center=call_center),
        CallCenterUserFactory.create(call_center=call_center),
    ]

    calls = [
        CallFactory.create(call_center=call_center_setup.call_center),
        CallFactory.create(call_center=call_center_setup.call_center),
        CallFactory.create(call_center=call_center_setup.call_center),
    ]
    event_ids = [call.id for call in calls]
    call_ids = [call.external_id for call in calls]

    # Listener configuration
    sqs_listener.register_rule(
        test_events_client,
        test_event_bus,
        {"source": [EVENT_IDENTIFIER], "detail-type": [RecordingProcessEvent.__name__]},
    )

    settings = TalkdeskDownloadSettings(
        event_identifier=EVENT_IDENTIFIER,
        event_bus_name="test-lambda",
        rds_secret_name="test-postgres-credentials",
        talkdesk_api_url="https://test.talkdeskapp.com/api/v2",
        talkdesk_secret_arn="arn:aws:secretsmanager:us-west-2:123456789012:secret:talkdesk-test-secret",
    )

    return TalkDeskDownloadFixture(
        session=base_session,
        event_listener=sqs_listener,
        users=users,
        calls=calls,
        settings=settings,
        events_client=events_client,
        event_ids=event_ids,
        call_ids=call_ids,
        user=user,
        call_ctr_path=call_center_setup.call_center.s3_storage_path,
    )


class TestTalkdeskDownload:
    """Test suite for talkdesk_download lambda handler."""

    def test_successful_download_and_processing(self, talkdesk_download_fixture):
        """Test successful download and processing of Talkdesk recordings."""
        with requests_mock.Mocker() as m:
            # Mock token endpoint
            m.post("https://test.talkdeskapp.com/oauth/token", json={"access_token": "test_bearer_token"})

            # Mock recordings API for each call
            for call_id in talkdesk_download_fixture.call_ids:
                m.get(
                    f"https://test.talkdeskapp.com/api/v2/calls/{call_id}/recordings",
                    json={
                        "total_pages": 1,
                        "_embedded": {
                            "recordings": [
                                {
                                    "id": f"rec_{call_id}_1",
                                    "created_at": "2023-01-01T12:00:00Z",
                                    "index": 1,
                                    "duration": 300,
                                    "agents": {
                                        "channel_2": [{"participant_id": talkdesk_download_fixture.user.external_id}]
                                    },
                                    "_links": {
                                        "media": {"href": f"https://test.talkdeskapp.com/api/v2/media/rec_{call_id}"}
                                    },
                                },
                                {
                                    "id": f"rec_{call_id}_2",
                                    "created_at": "2023-01-01T12:00:00Z",
                                    "index": 2,
                                    "duration": 300,
                                    "agents": {
                                        "channel_2": [{"participant_id": talkdesk_download_fixture.user.external_id}]
                                    },
                                    "_links": {
                                        "media": {"href": f"https://test.talkdeskapp.com/api/v2/media/rec_{call_id}"}
                                    },
                                },
                            ]
                        },
                    },
                )

                # Mock media download
                m.get(
                    f"https://test.talkdeskapp.com/api/v2/media/rec_{call_id}",
                    content=b"fake audio content",
                    headers={
                        "Content-Type": "audio/mp4",
                        "Content-Disposition": f'attachment; filename="recording_{call_id}.mp4"',
                    },
                )

            tracker = create_test_tracker(talkdesk_download_fixture.event_ids)
            run_all(
                tracker=tracker,
                eventbridge_client=talkdesk_download_fixture.events_client,
                db_session=talkdesk_download_fixture.session,
                settings=talkdesk_download_fixture.settings,
                token="test_bearer_token",
            )

            # Check response from tracker
            response = tracker.generate_response()
            assert response["statusCode"] == 200
            assert response["stats"]["succeeded"] == 3  # 3 calls processed successfully
            assert response["stats"]["failed"] == 0

            # Check that calls were updated with audio URLs
            updated_recordings = talkdesk_download_fixture.session.scalars(
                select(Recording).where(Recording.id.in_([1, 2, 3, 4, 5, 6]))
            ).all()

            assert len(updated_recordings) == 6
            for recording in updated_recordings:
                assert recording.audio_url is not None
                assert f"recordings/{recording.external_id}/" in recording.audio_url

            # Check that next events were published (one per successful call)
            received_messages = talkdesk_download_fixture.event_listener.poll()
            assert len(received_messages) == 6

            # Check that each message has the correct format
            published_event_ids = set()
            for message in received_messages:
                event_body = json.loads(message["Body"])
                parsed_message = EventBridgeMessage.from_bridge_event(event_body, RecordingProcessEvent)

                assert parsed_message.source == EVENT_IDENTIFIER

                detail: RecordingProcessEvent = parsed_message.detail
                assert detail.event_type == CallProcessStatus.DOWNLOADED
                published_event_ids.add(detail.recording_id)

            # Verify all successful call IDs were published
            assert published_event_ids == {1, 2, 3, 4, 5, 6}

            # Check CallProcess records were created for tracking download progress
            call_processes = talkdesk_download_fixture.session.scalars(select(CallProcess)).all()

            # Should have DOWNLOAD_STARTED records for each call,
            # RECORDING_DOWNLOADED records for each recording,
            # and DOWNLOADED records for each call
            download_started_processes = [
                cp for cp in call_processes if cp.process_status == ProcessStatus.DOWNLOAD_STARTED
            ]
            assert len(download_started_processes) == 3  # One per call

            recording_downloaded_processes = [
                cp for cp in call_processes if cp.process_status == ProcessStatus.RECORDING_DOWNLOADED
            ]
            assert len(recording_downloaded_processes) == 6  # One per recording

            call_downloaded_processes = [cp for cp in call_processes if cp.process_status == ProcessStatus.DOWNLOADED]
            assert len(call_downloaded_processes) == 3  # One per call

            # Verify all CallProcess records have the correct source

            for cp in call_processes:
                assert cp.status_source == EVENT_IDENTIFIER
                assert cp.call_id in [1, 2, 3]  # All should be associated with test calls

            # Verify recording downloaded processes have recording_ids
            for cp in recording_downloaded_processes:
                assert cp.recording_id is not None
                assert cp.recording_id in [1, 2, 3, 4, 5, 6]

            # Assert we have all the sources
            call_ids = [cp.call_id for cp in download_started_processes]
            call_ids.sort()
            assert [1, 2, 3] == call_ids

            recording_ids = [cp.recording_id for cp in recording_downloaded_processes]
            recording_ids.sort()
            assert [1, 2, 3, 4, 5, 6] == recording_ids

    def test_partial_failure_handling(self, talkdesk_download_fixture):
        """Test handling of partial failures in call processing."""
        call_1, call_2, call_3 = talkdesk_download_fixture.calls

        with requests_mock.Mocker() as m:
            # Mock token endpoint
            m.post("https://test.talkdeskapp.com/oauth/token", json={"access_token": "test_bearer_token"})

            # Mock successful recording for call_1
            m.get(
                f"https://test.talkdeskapp.com/api/v2/calls/{call_1.external_id}/recordings",
                json={
                    "total_pages": 1,
                    "_embedded": {
                        "recordings": [
                            {
                                "id": "rec_call_1",
                                "created_at": "2023-01-01T12:00:00Z",
                                "index": 1,
                                "duration": 300,
                                "agents": {
                                    "channel_2": [{"participant_id": talkdesk_download_fixture.user.external_id}]
                                },
                                "_links": {"media": {"href": "https://test.talkdeskapp.com/api/v2/media/rec_call_1"}},
                            }
                        ]
                    },
                },
            )

            m.get(
                "https://test.talkdeskapp.com/api/v2/media/rec_call_1",
                content=b"fake audio content",
                headers={"Content-Type": "audio/mp4"},
            )

            # Mock failures for call_2 and call_3
            m.get(f"https://test.talkdeskapp.com/api/v2/calls/{call_2.external_id}/recordings", status_code=500)
            m.get(f"https://test.talkdeskapp.com/api/v2/calls/{call_3.external_id}/recordings", status_code=500)

            tracker = create_test_tracker(talkdesk_download_fixture.event_ids)
            response = run_all(
                tracker=tracker,
                eventbridge_client=talkdesk_download_fixture.events_client,
                db_session=talkdesk_download_fixture.session,
                settings=talkdesk_download_fixture.settings,
                token="test_bearer_token",
            )

            # Check response indicates partial failure
            print(response)
            assert response["statusCode"] == 207  # Multi-status
            assert response["stats"]["succeeded"] == 1  # 1 call succeeded
            assert response["stats"]["failed"] == 2  # 2 calls failed

            # Check that only successful call was updated
            updated_calls = talkdesk_download_fixture.session.scalars(
                select(Recording).where(Recording.id.in_([1, 2, 3]))
            ).all()

            assert len(updated_calls) == 1
            successful_calls = [i for i in updated_calls if i.audio_url is not None]
            assert len(successful_calls) == 1
            assert successful_calls[0].external_id == "rec_call_1"

            # Check that one event was published for the successful call
            received_messages = talkdesk_download_fixture.event_listener.poll(buffer_time=1, wait_time=1)
            assert len(received_messages) == 1

            message = received_messages[0]
            event_body = json.loads(message["Body"])
            parsed_message = EventBridgeMessage.from_bridge_event(event_body, RecordingProcessEvent)

            assert parsed_message.source == EVENT_IDENTIFIER
            recording_detail = parsed_message.detail
            assert recording_detail.event_type == CallProcessStatus.DOWNLOADED
            assert recording_detail.recording_id == 1  # Should be the successful call ID

            # Check CallProcess records for tracking download progress
            call_processes = talkdesk_download_fixture.session.scalars(select(CallProcess)).all()

            # Should have DOWNLOAD_STARTED for call 1, RECORDING_DOWNLOADED for the recording,
            # DOWNLOADED for call completion, and FAILED for calls 2 and 3
            download_started_processes = [
                cp for cp in call_processes if cp.process_status == ProcessStatus.DOWNLOAD_STARTED
            ]
            assert len(download_started_processes) == 1  # Only for successful call
            assert download_started_processes[0].call_id == 1

            recording_downloaded_processes = [
                cp for cp in call_processes if cp.process_status == ProcessStatus.RECORDING_DOWNLOADED
            ]
            assert len(recording_downloaded_processes) == 1  # One recording for successful call
            assert recording_downloaded_processes[0].call_id == 1
            assert recording_downloaded_processes[0].recording_id == 1

            call_downloaded_processes = [cp for cp in call_processes if cp.process_status == ProcessStatus.DOWNLOADED]
            assert len(call_downloaded_processes) == 1  # One call completion for successful call
            assert call_downloaded_processes[0].call_id == 1

            failed_processes = [cp for cp in call_processes if cp.process_status == ProcessStatus.FAILED]
            assert len(failed_processes) == 2  # Two failed calls
            failed_call_ids = {cp.call_id for cp in failed_processes}
            assert failed_call_ids == {2, 3}

            # Verify all CallProcess records have the correct source
            for cp in call_processes:
                assert cp.status_source == EVENT_IDENTIFIER

    def test_no_recordings_found(
        self,
        talkdesk_download_fixture,
        encryption_secret,
    ):
        """Test handling when no recordings are found for calls."""

        with requests_mock.Mocker() as m:
            # Mock token endpoint
            m.post("https://test.talkdeskapp.com/oauth/token", json={"access_token": "test_bearer_token"})

            # Mock empty recordings response for all calls
            for call_id in talkdesk_download_fixture.call_ids:
                m.get(
                    f"https://test.talkdeskapp.com/api/v2/calls/{call_id}/recordings",
                    json={"total_pages": 1, "_embedded": {"recordings": []}},
                )

            tracker = create_test_tracker(talkdesk_download_fixture.event_ids)
            run_all(
                tracker=tracker,
                eventbridge_client=talkdesk_download_fixture.events_client,
                db_session=talkdesk_download_fixture.session,
                settings=talkdesk_download_fixture.settings,
                token="test_bearer_token",
            )

            # Check response
            response = tracker.generate_response()
            assert response["statusCode"] == 500  # All failed
            assert response["stats"]["succeeded"] == 0
            assert response["stats"]["failed"] == 3

            # Check CallProcess records - should have FAILED status for all calls with no recordings
            call_processes = talkdesk_download_fixture.session.scalars(select(CallProcess)).all()

            failed_processes = [cp for cp in call_processes if cp.process_status == ProcessStatus.FAILED]
            assert len(failed_processes) == 3  # All calls failed (no recordings found)

            failed_call_ids = {cp.call_id for cp in failed_processes}
            assert failed_call_ids == set(talkdesk_download_fixture.event_ids)

            # Verify all CallProcess records have the correct source and status text
            for cp in failed_processes:
                assert cp.status_source == EVENT_IDENTIFIER
                assert cp.status_text.message == "No downloads found"

    def test_recordings_without_agent_skipped(
        self,
        talkdesk_download_fixture,
        encryption_secret,
    ):
        """Test that recordings without assigned agents are skipped."""
        call_1, call_2, call_3 = talkdesk_download_fixture.calls
        event_ids = [call_1.id]

        with requests_mock.Mocker() as m:
            # Mock token endpoint
            m.post("https://test.talkdeskapp.com/oauth/token", json={"access_token": "test_bearer_token"})

            # Mock recordings response with no agent assigned
            m.get(
                f"https://test.talkdeskapp.com/api/v2/calls/{call_1.external_id}/recordings",
                json={
                    "total_pages": 1,
                    "_embedded": {
                        "recordings": [
                            {
                                "id": "rec_no_agent",
                                "created_at": "2023-01-01T12:00:00Z",
                                "index": 1,
                                "duration": 300,
                                "agents": {"channel_2": []},  # No agent assigned
                                "_links": {"media": {"href": "https://test.talkdeskapp.com/api/v2/media/rec_no_agent"}},
                            }
                        ]
                    },
                },
            )

            tracker = create_test_tracker(event_ids)
            run_all(
                tracker=tracker,
                eventbridge_client=talkdesk_download_fixture.events_client,
                db_session=talkdesk_download_fixture.session,
                settings=talkdesk_download_fixture.settings,
                token="test_bearer_token",
            )

            # Check that no recordings were processed (agent was skipped)
            response = tracker.generate_response()
            assert response["statusCode"] == 500  # Failed because no recordings found
            assert response["stats"]["succeeded"] == 0  # Call failed due to no recordings found
            assert response["stats"]["failed"] == 1

            # Check CallProcess records - should have DOWNLOAD_STARTED but no DOWNLOADED records
            call_processes = talkdesk_download_fixture.session.scalars(select(CallProcess)).all()

            download_started_processes = [
                cp for cp in call_processes if cp.process_status == ProcessStatus.DOWNLOAD_STARTED
            ]
            assert len(download_started_processes) == 1  # Call processing started
            assert download_started_processes[0].call_id == call_1.id

            # No downloaded processes since recording was skipped due to missing agent
            downloaded_processes = [
                cp for cp in call_processes if cp.process_status == ProcessStatus.RECORDING_DOWNLOADED
            ]
            assert len(downloaded_processes) == 0

            # Verify CallProcess records have the correct source
            for cp in call_processes:
                assert cp.status_source == EVENT_IDENTIFIER

    @pytest.mark.parametrize(
        "event_data,expected_status,expected_error",
        [
            (
                # Missing detail field
                {},
                500,
                "Unable to process lambda event",
            ),
            (
                # Wrong event type
                {
                    "source": EVENT_IDENTIFIER,
                    "detail-type": "IncomingCalls",
                    "detail": {"version": "1.0", "event_type": "wrong_type", "event_id": 1},
                },
                500,
                "Unable to process lambda event",
            ),
            (
                # Non-existent call IDs
                {
                    "source": EVENT_IDENTIFIER,
                    "detail-type": "IncomingCalls",
                    "detail": {"version": "1.0", "event_type": "created", "event_id": 999},
                },
                500,
                "Unable to process lambda event",
            ),
        ],
    )
    def test_invalid_event(
        self,
        talkdesk_lambda_env,
        base_session,
        db_secret,
        talkdesk_secret,
        event_data,
        expected_status,
        expected_error,
    ):
        """Test various invalid event scenarios."""
        result = handler(event_data, {})

        assert result["statusCode"] == expected_status
        response_body = json.loads(result["body"])
        assert expected_error in response_body["message"]

    def test_token_failure(self, talkdesk_download_fixture):
        """Test handling of token acquisition failure."""
        incoming_calls_detail = IncomingCall(event_type=CallProcessStatus.CREATED, event_id=1)
        event = create_test_event(incoming_calls_detail)

        with requests_mock.Mocker() as m:
            # Mock failed token response
            m.post(talkdesk_download_fixture.settings.api_url, status_code=401)

            result = handler(event, {})
            assert result["statusCode"] == 500


class TestTalkdeskDownloadHelpers:
    """Test suite for helper functions."""

    def test_get_bearer_token_success(self, talkdesk_lambda_env):
        """Test successful bearer token acquisition."""

        # Set up the secret in the mock secrets manager using the ARN from talkdesk_lambda_env
        talkdesk_secret = TalkdeskSecret(
            client_id="test_client_id",
            client_secret="test_client_secret",
            token_url="https://test.talkdeskapp.com/oauth/token",
        )

        # Create TalkdeskDownloadSettings (will read from environment variables set by talkdesk_lambda_env)

        with requests_mock.Mocker() as m:
            m.post("https://test.talkdeskapp.com/oauth/token", json={"access_token": "test_token_123"})

            token = get_bearer_token(talkdesk_secret)
            assert token == "test_token_123"

    def test_get_bearer_token_failure(self, talkdesk_lambda_env):
        """Test bearer token acquisition failure."""

        # Set up the secret in the mock secrets manager using the ARN from talkdesk_lambda_env
        talkdesk_secret = TalkdeskSecret(
            client_id="test_client_id",
            client_secret="test_client_secret",
            token_url="https://test.talkdeskapp.com/oauth/token",
        )
        # Use the secret name that matches what's in talkdesk_lambda_env

        # Create TalkdeskDownloadSettings (will read from environment variables set by talkdesk_lambda_env)

        with requests_mock.Mocker() as m:
            m.post("https://test.talkdeskapp.com/oauth/token", status_code=401)

            with pytest.raises(PipelineProcessingException, match="Failed to obtain bearer token"):
                get_bearer_token(talkdesk_secret)

    def test_extract_filename_from_content_disposition(self):
        """Test filename extraction from Content-Disposition header."""

        class MockResponse:
            headers = {"Content-Disposition": 'attachment; filename="test_recording.mp4"'}

        filename = extract_filename(MockResponse(), "rec_123")
        assert filename == "test_recording.mp4"

    def test_extract_filename_from_content_type(self):
        """Test filename generation from Content-Type header."""

        class MockResponse:
            headers = {"Content-Type": "audio/mpeg"}

        filename = extract_filename(MockResponse(), "rec_123")
        assert filename == "rec_123.mp3"

    def test_extract_filename_fallback(self):
        """Test filename generation when no headers provide information."""

        class MockResponse:
            headers = {}

        filename = extract_filename(MockResponse(), "rec_123")
        assert filename == "rec_123.mp4"

    def test_upload_media_to_s3_with_retries(self, talkdesk_download_fixture):
        """Test media upload with retry logic."""

        with requests_mock.Mocker() as m:
            # First two requests fail, third succeeds
            m.get(
                "https://media.url",
                [
                    {"status_code": 500},
                    {"status_code": 500},
                    {"content": b"fake audio content", "headers": {"Content-Type": "audio/mp4"}},
                ],
            )

            with patch("time.sleep"):  # Speed up test
                s3_path = S3Path(talkdesk_download_fixture.call_ctr_path)
                result_path = upload_media_to_s3(
                    "https://media.url", "bearer_token", "call_123", "rec_456", s3_path, 3, 30
                )

                assert result_path.startswith(
                    f"{talkdesk_download_fixture.call_ctr_path}/calls/call_123/recordings/rec_456/"
                )

    def test_upload_media_to_s3_max_retries_exceeded(self, talkdesk_download_fixture):
        """Test media upload failure after max retries."""

        with requests_mock.Mocker() as m:
            # All requests fail
            m.get("https://media.url", status_code=500)

            with patch("time.sleep"):
                s3_path = S3Path(talkdesk_download_fixture.call_ctr_path)
                with pytest.raises(PipelineProcessingException, match="Failed to upload recording"):
                    upload_media_to_s3("https://media.url", "bearer_token", "call_123", "rec_456", s3_path, 3, 30)

    def test_process_call_success(
        self,
        talkdesk_download_fixture,
    ):
        """Test successful call processing."""

        call = talkdesk_download_fixture.calls[0]

        with requests_mock.Mocker() as m:
            # Mock recordings API
            m.get(
                f"{talkdesk_download_fixture.settings.api_url}/calls/{call.external_id}/recordings",
                json={
                    "total_pages": 1,
                    "_embedded": {
                        "recordings": [
                            {
                                "id": "rec_1",
                                "created_at": "2023-01-01T12:00:00Z",
                                "agents": {
                                    "channel_2": [{"participant_id": talkdesk_download_fixture.users[0].external_id}]
                                },
                                "_links": {
                                    "media": {"href": f"{talkdesk_download_fixture.settings.api_url}/media/rec_1"}
                                },
                            },
                            {
                                "id": "rec_2",
                                "created_at": "2023-01-01T12:05:00Z",
                                "agents": {
                                    "channel_2": [{"participant_id": talkdesk_download_fixture.users[1].external_id}]
                                },
                                "_links": {
                                    "media": {"href": f"{talkdesk_download_fixture.settings.api_url}/media/rec_2"}
                                },
                            },
                        ]
                    },
                },
            )

            # Mock media downloads
            m.get(
                f"{talkdesk_download_fixture.settings.api_url}/media/rec_1",
                content=b"fake audio content 1",
                headers={"Content-Type": "audio/mp4"},
            )
            m.get(
                f"{talkdesk_download_fixture.settings.api_url}/media/rec_2",
                content=b"fake audio content 2",
                headers={"Content-Type": "audio/mp4"},
            )

            success = run_thread(
                talkdesk_download_fixture.session.bind,
                call.id,
                "bearer_token",
                talkdesk_download_fixture.settings,
                talkdesk_download_fixture.events_client,
            )

            # Check the outputs
            assert success is True
            assert len(call.recordings) == 2

            # Verify all recordings have audio URLs
            recording_ids_in_db = {rec.id for rec in call.recordings}
            assert len(recording_ids_in_db) == 2

            # Check CallProcess records were created for tracking download progress
            call_processes = talkdesk_download_fixture.session.scalars(
                select(CallProcess).where(CallProcess.call_id == call.id)
            ).all()

            # Should have DOWNLOAD_STARTED record for the call, RECORDING_DOWNLOADED records for each recording,
            # and DOWNLOADED record for call completion
            download_started_processes = [
                cp for cp in call_processes if cp.process_status == ProcessStatus.DOWNLOAD_STARTED
            ]
            assert len(download_started_processes) == 1
            assert download_started_processes[0].call_id == call.id

            recording_downloaded_processes = [
                cp for cp in call_processes if cp.process_status == ProcessStatus.RECORDING_DOWNLOADED
            ]
            assert len(recording_downloaded_processes) == 2  # One per recording

            call_downloaded_processes = [cp for cp in call_processes if cp.process_status == ProcessStatus.DOWNLOADED]
            assert len(call_downloaded_processes) == 1  # One per call completion
            assert call_downloaded_processes[0].call_id == call.id

            # Verify all CallProcess records have the correct source and recording IDs
            for cp in call_processes:
                assert cp.status_source == talkdesk_download_fixture.settings.event_identifier
                assert cp.call_id == call.id

            # Verify recording downloaded processes have recording_ids
            for cp in recording_downloaded_processes:
                assert cp.recording_id is not None
                assert cp.recording_id in recording_ids_in_db

    def test_process_call_no_recordings(
        self,
        talkdesk_download_fixture,
    ):
        """Test call processing when no recordings are found."""

        call = talkdesk_download_fixture.calls[0]

        with requests_mock.Mocker() as m:
            m.get(
                f"{talkdesk_download_fixture.settings.api_url}/calls/{call.external_id}/recordings",
                json={"total_pages": 1, "_embedded": {"recordings": []}},
            )
            base_engine = talkdesk_download_fixture.session.bind

            success = run_thread(
                base_engine,
                call.id,
                "bearer_token",
                talkdesk_download_fixture.settings,
                talkdesk_download_fixture.events_client,
            )

            # When no recordings are found, should return False
            assert success is False

    def test_process_call_api_failure(self, talkdesk_download_fixture):
        """Test call processing when API call fails."""
        call = talkdesk_download_fixture.calls[0]

        with requests_mock.Mocker() as m:
            m.get(
                f"{talkdesk_download_fixture.settings.api_url}/calls/{call.external_id}/recordings",
                status_code=500,
            )

            # API failure should return False and log error
            base_engine = talkdesk_download_fixture.session.bind
            success = run_thread(
                base_engine,
                call.id,
                "bearer_token",
                talkdesk_download_fixture.settings,
                talkdesk_download_fixture.events_client,
            )
            assert success is False

            # Check that a FAILED CallProcess was created
            call_process = talkdesk_download_fixture.session.scalars(
                select(CallProcess).where(
                    CallProcess.call_id == call.id,
                    CallProcess.process_status == ProcessStatus.FAILED,
                )
            ).first()
            assert call_process is not None
            assert call_process.status_text.message == "No downloads found"
            assert call_process.status_text.exception is None

    def test_process_call_with_pagination(
        self,
        talkdesk_download_fixture,
    ):
        """Test call processing with multiple pages of recordings."""

        call = talkdesk_download_fixture.calls[0]

        with requests_mock.Mocker() as m:
            # Mock first page of recordings
            m.get(
                f"{talkdesk_download_fixture.settings.api_url}/calls/{call.external_id}/recordings",
                json={
                    "total_pages": 3,
                    "page": 1,
                    "per_page": 2,
                    "_embedded": {
                        "recordings": [
                            {
                                "id": "rec_page_1_1",
                                "created_at": "2023-01-01T12:00:00Z",
                                "index": 1,
                                "duration": 300,
                                "agents": {
                                    "channel_2": [{"participant_id": talkdesk_download_fixture.user.external_id}]
                                },
                                "_links": {
                                    "media": {
                                        "href": f"{talkdesk_download_fixture.settings.api_url}/media/rec_page_1_1"
                                    }
                                },
                            },
                            {
                                "id": "rec_page_1_2",
                                "created_at": "2023-01-01T12:05:00Z",
                                "index": 2,
                                "duration": 250,
                                "agents": {
                                    "channel_2": [{"participant_id": talkdesk_download_fixture.user.external_id}]
                                },
                                "_links": {
                                    "media": {
                                        "href": f"{talkdesk_download_fixture.settings.api_url}/media/rec_page_1_2"
                                    }
                                },
                            },
                        ]
                    },
                    "_links": {
                        "next": {
                            "href": f"{talkdesk_download_fixture.settings.api_url}/calls/{call.external_id}"
                            + "/recordings?page=2"
                        }
                    },
                },
            )

            # Mock second page of recordings
            m.get(
                f"{talkdesk_download_fixture.settings.api_url}/calls/{call.external_id}/recordings?page=2",
                json={
                    "total_pages": 3,
                    "page": 2,
                    "per_page": 2,
                    "_embedded": {
                        "recordings": [
                            {
                                "id": "rec_page_2_1",
                                "created_at": "2023-01-01T12:10:00Z",
                                "index": 3,
                                "duration": 180,
                                "agents": {
                                    "channel_2": [{"participant_id": talkdesk_download_fixture.user.external_id}]
                                },
                                "_links": {
                                    "media": {
                                        "href": f"{talkdesk_download_fixture.settings.api_url}/media/rec_page_2_1"
                                    }
                                },
                            },
                            {
                                "id": "rec_page_2_2",
                                "created_at": "2023-01-01T12:15:00Z",
                                "index": 4,
                                "duration": 320,
                                "agents": {
                                    "channel_2": [{"participant_id": talkdesk_download_fixture.user.external_id}]
                                },
                                "_links": {
                                    "media": {
                                        "href": f"{talkdesk_download_fixture.settings.api_url}/media/rec_page_2_2"
                                    }
                                },
                            },
                        ]
                    },
                    "_links": {
                        "next": {
                            "href": f"{talkdesk_download_fixture.settings.api_url}/calls/{call.external_id}/"
                            + "recordings?page=3"
                        }
                    },
                },
            )

            # Mock third (final) page of recordings
            m.get(
                f"{talkdesk_download_fixture.settings.api_url}/calls/{call.external_id}/recordings?page=3",
                json={
                    "total_pages": 3,
                    "page": 3,
                    "per_page": 2,
                    "_embedded": {
                        "recordings": [
                            {
                                "id": "rec_page_3_1",
                                "created_at": "2023-01-01T12:20:00Z",
                                "index": 5,
                                "duration": 275,
                                "agents": {
                                    "channel_2": [{"participant_id": talkdesk_download_fixture.user.external_id}]
                                },
                                "_links": {
                                    "media": {
                                        "href": f"{talkdesk_download_fixture.settings.api_url}/media/rec_page_3_1"
                                    }
                                },
                            }
                        ]
                    },
                    "_links": {},  # No next link on final page
                },
            )

            # Mock media downloads for all recordings
            for recording_id in ["rec_page_1_1", "rec_page_1_2", "rec_page_2_1", "rec_page_2_2", "rec_page_3_1"]:
                m.get(
                    f"{talkdesk_download_fixture.settings.api_url}/media/{recording_id}",
                    content=f"fake audio content {recording_id}".encode(),
                    headers={"Content-Type": "audio/mp4"},
                )

            # Mock EventBridge client for publishing events

            success = run_thread(
                talkdesk_download_fixture.session.bind,
                call.id,
                "bearer_token",
                talkdesk_download_fixture.settings,
                talkdesk_download_fixture.events_client,
            )

            assert success is True
            assert len(call.recordings) == 5

            # Verify all recording IDs are present
            recording_ids = {rec.external_id for rec in call.recordings}
            expected_ids = {"rec_page_1_1", "rec_page_1_2", "rec_page_2_1", "rec_page_2_2", "rec_page_3_1"}
            assert recording_ids == expected_ids

            # Verify all recordings have audio URLs
            for recording in call.recordings:
                assert recording.audio_url is not None
                assert f"recordings/{recording.external_id}/" in recording.audio_url, (
                    f"Expected recording path to contain 'recordings/{recording.external_id}/' "
                    f"but got {recording.audio_url}"
                )

            # Verify recording IDs in the database
            recording_ids_in_db = {rec.id for rec in call.recordings}
            assert len(recording_ids_in_db) == 5

            # Verify the API was called 3 times (once for each page)
            history = m.request_history
            recording_api_calls = [req for req in history if "/recordings" in req.url and req.method == "GET"]
            assert len(recording_api_calls) == 3

            # Verify the sequence of API calls
            assert "page=2" in recording_api_calls[1].url
            assert "page=3" in recording_api_calls[2].url

            # Check CallProcess records for pagination test
            call_processes = talkdesk_download_fixture.session.scalars(
                select(CallProcess).where(CallProcess.call_id == call.id)
            ).all()

            # Should have DOWNLOAD_STARTED record for the call, RECORDING_DOWNLOADED records for each recording,
            # and DOWNLOADED record for call completion
            download_started_processes = [
                cp for cp in call_processes if cp.process_status == ProcessStatus.DOWNLOAD_STARTED
            ]
            assert len(download_started_processes) == 1
            assert download_started_processes[0].call_id == call.id

            recording_downloaded_processes = [
                cp for cp in call_processes if cp.process_status == ProcessStatus.RECORDING_DOWNLOADED
            ]
            assert len(recording_downloaded_processes) == 5  # One per recording across all pages

            call_downloaded_processes = [cp for cp in call_processes if cp.process_status == ProcessStatus.DOWNLOADED]
            assert len(call_downloaded_processes) == 1  # One per call completion
            assert call_downloaded_processes[0].call_id == call.id

            # Verify all CallProcess records have the correct source
            for cp in call_processes:
                assert cp.status_source == talkdesk_download_fixture.settings.event_identifier
                assert cp.call_id == call.id

            # Verify recording downloaded processes have recording_ids
            for cp in recording_downloaded_processes:
                assert cp.recording_id is not None
                assert cp.recording_id in recording_ids_in_db

    def test_process_call_pagination_edge_cases(
        self,
        talkdesk_download_fixture,
    ):
        """Test pagination edge cases: single page, empty pages, no next link."""
        call = talkdesk_download_fixture.calls[0]

        with requests_mock.Mocker() as m:
            # Test single page with no next link
            m.get(
                f"{talkdesk_download_fixture.settings.api_url}/calls/{call.external_id}/recordings",
                json={
                    "total_pages": 1,
                    "page": 1,
                    "per_page": 10,
                    "_embedded": {
                        "recordings": [
                            {
                                "id": "rec_single_page",
                                "created_at": "2023-01-01T12:00:00Z",
                                "index": 1,
                                "duration": 300,
                                "agents": {
                                    "channel_2": [{"participant_id": talkdesk_download_fixture.user.external_id}]
                                },
                                "_links": {
                                    "media": {
                                        "href": f"{talkdesk_download_fixture.settings.api_url}/media/rec_single_page"
                                    }
                                },
                            }
                        ]
                    },
                    "_links": {},  # No next link - should stop pagination
                },
            )

            # Mock media download
            m.get(
                f"{talkdesk_download_fixture.settings.api_url}/media/rec_single_page",
                content=b"fake audio content",
                headers={"Content-Type": "audio/mp4"},
            )

            success = run_thread(
                talkdesk_download_fixture.session.bind,
                call.id,
                "bearer_token",
                talkdesk_download_fixture.settings,
                talkdesk_download_fixture.events_client,
            )

            # Should work with single page
            assert success is True
            assert len(call.recordings) == 1
            assert call.recordings[0].external_id == "rec_single_page"

            # Verify the recording ID in the database
            recording_ids_in_db = {rec.id for rec in call.recordings}
            assert len(recording_ids_in_db) == 1

            # Should only call API once
            history = m.request_history
            recording_api_calls = [req for req in history if "/recordings" in req.url and req.method == "GET"]
            assert len(recording_api_calls) == 1

            # Check CallProcess records for edge case test
            call_processes = talkdesk_download_fixture.session.scalars(
                select(CallProcess).where(CallProcess.call_id == call.id)
            ).all()

            # Should have DOWNLOAD_STARTED record for the call, RECORDING_DOWNLOADED record for the recording,
            # and DOWNLOADED record for call completion
            download_started_processes = [
                cp for cp in call_processes if cp.process_status == ProcessStatus.DOWNLOAD_STARTED
            ]
            assert len(download_started_processes) == 1
            assert download_started_processes[0].call_id == call.id

            recording_downloaded_processes = [
                cp for cp in call_processes if cp.process_status == ProcessStatus.RECORDING_DOWNLOADED
            ]
            assert len(recording_downloaded_processes) == 1  # One recording
            assert recording_downloaded_processes[0].recording_id in recording_ids_in_db

            call_downloaded_processes = [cp for cp in call_processes if cp.process_status == ProcessStatus.DOWNLOADED]
            assert len(call_downloaded_processes) == 1  # One call completion
            assert call_downloaded_processes[0].call_id == call.id

            # Verify all CallProcess records have the correct source
            for cp in call_processes:
                assert cp.status_source == talkdesk_download_fixture.settings.event_identifier
                assert cp.call_id == call.id

    def test_duplicate_recording_prevention(self, talkdesk_download_fixture):
        """Test that recordings with same external_id and call_id are not duplicated."""
        # First, create an existing recording in the database
        call = talkdesk_download_fixture.calls[1]
        record = RecordingFactory.create(
            call=call,
            external_id="rec_call_1_1",
            call_center_user=talkdesk_download_fixture.user,
            audio_url="s3://test-bucket/existing.mp4",
        )

        event_ids = [talkdesk_download_fixture.event_ids[1]]  # Only process call_1

        with requests_mock.Mocker() as m:
            # Mock token endpoint
            m.post("https://test.talkdeskapp.com/oauth/token", json={"access_token": "test_bearer_token"})

            # Mock recordings API for call_1 - returning the same external_id as existing recording
            m.get(
                f"https://test.talkdeskapp.com/api/v2/calls/{talkdesk_download_fixture.call_ids[1]}/recordings",
                json={
                    "total_pages": 1,
                    "_embedded": {
                        "recordings": [
                            {
                                "id": "rec_call_1_1",  # Same external_id as existing recording - should be skipped
                                "created_at": "2023-01-01T12:00:00Z",
                                "index": 1,
                                "duration": 300,
                                "agents": {
                                    "channel_2": [{"participant_id": talkdesk_download_fixture.user.external_id}]
                                },
                                "_links": {"media": {"href": "https://test.talkdeskapp.com/api/v2/media/rec_call_1_1"}},
                            },
                            {
                                "id": "rec_call_1_2",  # Different external_id - should be processed
                                "created_at": "2023-01-01T12:00:00Z",
                                "index": 2,
                                "duration": 300,
                                "agents": {
                                    "channel_2": [{"participant_id": talkdesk_download_fixture.user.external_id}]
                                },
                                "_links": {"media": {"href": "https://test.talkdeskapp.com/api/v2/media/rec_call_1_2"}},
                            },
                        ]
                    },
                },
            )

            # Mock media download for the new recording only (duplicate won't be downloaded)
            m.get(
                "https://test.talkdeskapp.com/api/v2/media/rec_call_1_2",
                content=b"fake audio content",
                headers={
                    "Content-Type": "audio/mp4",
                    "Content-Disposition": 'attachment; filename="recording_call_1_2.mp4"',
                },
            )

            # Set allow_duplicate_records to False to test the duplicate prevention
            talkdesk_download_fixture.settings.allow_duplicate_records = False

            tracker = create_test_tracker(event_ids)
            run_all(
                tracker=tracker,
                eventbridge_client=talkdesk_download_fixture.events_client,
                db_session=talkdesk_download_fixture.session,
                settings=talkdesk_download_fixture.settings,
                token="test_bearer_token",
            )

        # Verify results
        recordings = talkdesk_download_fixture.session.scalars(
            select(Recording).where(Recording.call_id == record.call_id)
        ).all()

        # Should have 2 recordings total: the existing one + the new one (duplicate was skipped)
        assert len(recordings) == 2

        external_ids = [r.external_id for r in recordings]
        assert "rec_call_1_1" in external_ids  # Existing recording
        assert "rec_call_1_2" in external_ids  # New recording

        # Verify that the existing recording wasn't modified
        existing_rec = next(r for r in recordings if r.external_id == "rec_call_1_1")
        assert existing_rec.audio_url == "s3://test-bucket/existing.mp4"  # Original URL preserved

        # Verify that the new recording was created
        new_rec = next(r for r in recordings if r.external_id == "rec_call_1_2")
        assert new_rec.audio_url.startswith(talkdesk_download_fixture.call_ctr_path)
        assert "rec_call_1_2" in new_rec.audio_url

    def test_same_external_id_different_calls_allowed(self, talkdesk_download_fixture):
        """Test that recordings with same external_id but different call_id are allowed."""
        # Create an existing recording for call_1
        record = RecordingFactory.create(
            external_id="rec_123",  # Same external_id we'll use for call_2
            call_center_user=talkdesk_download_fixture.user,
        )

        event_ids = [talkdesk_download_fixture.event_ids[2]]  # Process call_2

        with requests_mock.Mocker() as m:
            # Mock token endpoint
            m.post("https://test.talkdeskapp.com/oauth/token", json={"access_token": "test_bearer_token"})

            # Mock recordings API for call_2 - using same external_id as call_1's recording
            m.get(
                f"https://test.talkdeskapp.com/api/v2/calls/{talkdesk_download_fixture.call_ids[2]}/recordings",
                json={
                    "total_pages": 1,
                    "_embedded": {
                        "recordings": [
                            {
                                "id": "rec_123",  # Same external_id as existing recording but different call
                                "created_at": "2023-01-01T13:00:00Z",
                                "index": 1,
                                "duration": 300,
                                "agents": {
                                    "channel_2": [{"participant_id": talkdesk_download_fixture.user.external_id}]
                                },
                                "_links": {
                                    "media": {"href": "https://test.talkdeskapp.com/api/v2/media/rec_123_call2"}
                                },
                            },
                        ]
                    },
                },
            )

            # Mock media download
            m.get(
                "https://test.talkdeskapp.com/api/v2/media/rec_123_call2",
                content=b"fake audio content for call 2",
                headers={
                    "Content-Type": "audio/mp4",
                    "Content-Disposition": 'attachment; filename="recording_call_2.mp4"',
                },
            )

            tracker = create_test_tracker(event_ids)
            run_all(
                tracker=tracker,
                eventbridge_client=talkdesk_download_fixture.events_client,
                db_session=talkdesk_download_fixture.session,
                settings=talkdesk_download_fixture.settings,
                token="test_bearer_token",
            )

        # Verify results
        all_recordings = talkdesk_download_fixture.session.scalars(select(Recording)).all()

        # Should have 2 recordings total: one for call_1 and one for call_2, both with same external_id
        assert len(all_recordings) == 2

        call_1_recordings = [r for r in all_recordings if r.call_id == record.call_id]
        call_2_recordings = [r for r in all_recordings if r.call_id == talkdesk_download_fixture.event_ids[2]]

        assert len(call_1_recordings) == 1
        assert len(call_2_recordings) == 1

        # Both should have the same external_id but different call_ids
        assert call_1_recordings[0].external_id == "rec_123"
        assert call_2_recordings[0].external_id == "rec_123"

        # Verify different audio URLs
        assert call_1_recordings[0].audio_url == record.audio_url
        assert call_2_recordings[0].audio_url.startswith(talkdesk_download_fixture.call_ctr_path)
        assert "rec_123" in call_2_recordings[0].audio_url
