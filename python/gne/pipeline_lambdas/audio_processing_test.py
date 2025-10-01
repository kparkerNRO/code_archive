import json
import os
from copy import copy
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from common.database.database_fixtures import RecordingFactory
from common.database.models import Call, CallCenterUser, CallProcess, DeepgramApiCall, Recording
from common.models.db_enum import ProcessStatus
from deepgram import PrerecordedOptions
from s3pathlib import S3Path
from sqlalchemy import select
from sqlalchemy.orm import Session

from data_pipeline.audio_processing import DeepgramSettings, format_speaker_transcript, run
from data_pipeline.conftest import SQSEventBridgeListener
from data_pipeline.shared.errors import PipelineProcessingException


@pytest.fixture()
def transcript_contents():
    # Have the json library validate and flatten
    return json.dumps(
        json.loads(
            """
{
  "metadata": {
    "transaction_key": "deprecated",
    "request_id": "f3d72d3c-fd34-4d9c-8d9c-e8f31a5c1fc9",
    "sha256": "d92a8e96dcaa21ebd60b42a92f09c009ca9e372c8c04c6a1bb3f4e819ac6d933",
    "created": "2025-03-24T07:52:04.602Z",
    "duration": 2942.528,
    "channels": 1,
    "models": [
      "349c83dc-989b-4a6c-9c67-a43abd0587d0"
    ],
    "model_info": {
      "349c83dc-989b-4a6c-9c67-a43abd0587d0": {
        "name": "2-medical-nova",
        "version": "2024-05-31.13574",
        "arch": "nova-2"
      }
    }
  },
  "results": {
    "channels": [
      {
        "alternatives": [
          {
            "transcript": "Okay, so I mentioned we're going to talk about food allergy.",
            "confidence": 0.99678016,
            "words": [
              {
                "word": "okay",
                "start": 2.48,
                "end": 2.98,
                "confidence": 0.81629366,
                "speaker": 0,
                "speaker_confidence": 0.43405098,
                "punctuated_word": "Okay,"
              },
              {
                "word": "so",
                "start": 3.36,
                "end": 3.6799998,
                "confidence": 0.9965522,
                "speaker": 0,
                "speaker_confidence": 0.43405098,
                "punctuated_word": "so"
              },
              {
                "word": "i",
                "start": 3.6799998,
                "end": 3.84,
                "confidence": 0.985838,
                "speaker": 0,
                "speaker_confidence": 0.43405098,
                "punctuated_word": "I"
              },
              {
                "word": "mentioned",
                "start": 3.84,
                "end": 4.08,
                "confidence": 0.9992514,
                "speaker": 0,
                "speaker_confidence": 0.43405098,
                "punctuated_word": "mentioned"
              },
              {
                "word": "we're",
                "start": 4.08,
                "end": 4.24,
                "confidence": 0.9613435,
                "speaker": 0,
                "speaker_confidence": 0.43405098,
                "punctuated_word": "we're"
              },
              {
                "word": "going",
                "start": 4.24,
                "end": 4.3199997,
                "confidence": 0.6987029,
                "speaker": 0,
                "speaker_confidence": 0.43405098,
                "punctuated_word": "going"
              },
              {
                "word": "to",
                "start": 4.3199997,
                "end": 4.4,
                "confidence": 0.9981799,
                "speaker": 0,
                "speaker_confidence": 0.43405098,
                "punctuated_word": "to"
              },
              {
                "word": "talk",
                "start": 4.4,
                "end": 4.56,
                "confidence": 0.9996543,
                "speaker": 0,
                "speaker_confidence": 0.43405098,
                "punctuated_word": "talk"
              },
              {
                "word": "about",
                "start": 4.56,
                "end": 4.88,
                "confidence": 0.9998642,
                "speaker": 0,
                "speaker_confidence": 0.5494979,
                "punctuated_word": "about"
              },
              {
                "word": "food",
                "start": 4.88,
                "end": 5.2799997,
                "confidence": 0.998585,
                "speaker": 0,
                "speaker_confidence": 0.5494979,
                "punctuated_word": "food"
              },
              {
                "word": "allergy",
                "start": 5.2799997,
                "end": 5.7799997,
                "confidence": 0.8933861,
                "speaker": 0,
                "speaker_confidence": 0.5494979,
                "punctuated_word": "allergy."
              }
            ],
            "paragraphs": {
              "transcript": "Speaker 0: Okay, so I mentioned we're going to talk about food allergy.",
              "paragraphs": [
                {
                  "sentences": [
                    {
                      "text": "Okay, so I mentioned we're going to talk about food allergy.",
                      "start": 2.48,
                      "end": 5.7799997
                    }
                  ],
                  "speaker": 0,
                  "num_words": 11,
                  "start": 2.48,
                  "end": 6.829998
                }
              ]
            }
          }
        ]
      }
    ]
  }
}
    """
        )
    )


@dataclass
class AudioProcessingFixture:
    base_session: Session
    event_listener: SQSEventBridgeListener
    deepgram_client: Any
    deepgram_options: PrerecordedOptions
    boto3_session: Any
    s3_client: Any
    settings: DeepgramSettings
    test_user: CallCenterUser
    call: Call
    s3_bucket: str


@pytest.fixture()
def processing_fixture(
    base_env,
    db_secret,
    base_session,
    boto3_session,
    transcript_contents,
    test_events_client,
    sqs_listener,
    test_event_bus,
    secrets_client,
    s3_client,
    encryption_secret,
    call_center_setup,
    call,
):
    # Deepgram secret
    secret_value = {
        "API_KEY": "test_deepgram_api_key",
        "HOSTNAME": "test-service-url",
    }
    secret_name = "deepgram_secrets"
    secrets_client.create_secret(Name=secret_name, SecretString=json.dumps(secret_value))

    # Settings
    settings = DeepgramSettings(
        event_identifier="test.data-pipeline",
        event_bus_name="test-lambda",
        rds_secret_name="test-postgres-credentials",
        deepgram_api_secret_name="deepgram_secrets",
    )

    deepgram_options = PrerecordedOptions(
        model="nova-2-medical",
        smart_format=True,
        diarize=True,
        multichannel=False,
    )

    # Event listener
    sqs_listener.register_rule(
        test_events_client,
        test_event_bus,
        {"source": ["test.data-pipeline"]},
    )

    # Deepgram client
    deepgram_client = MagicMock()
    deepgram_client.listen.rest.v.return_value.transcribe_file.return_value = json.loads(transcript_contents)

    with (
        patch("data_pipeline.audio_processing.sleep"),
        patch.dict(os.environ, {"DEEPGRAM_API_SECRET_NAME": secret_name}),
    ):
        yield AudioProcessingFixture(
            base_session=base_session,
            event_listener=sqs_listener,
            deepgram_client=deepgram_client,
            deepgram_options=deepgram_options,
            boto3_session=boto3_session,
            s3_client=s3_client,
            settings=settings,
            test_user=call_center_setup.agent,
            call=call,
            s3_bucket=call_center_setup.call_center.s3_storage_path,
        )


@pytest.fixture()
def recording(processing_fixture):
    return RecordingFactory.create_with_s3_data(
        s3_client=processing_fixture.s3_client,
        call_center_user=processing_fixture.test_user,
        call=processing_fixture.call,
        audio_content=b"test audio content",
        has_audio=True,
        has_transcript=False,
    )


def test_handler_base(
    processing_fixture,
    transcript_contents,
):
    recording = RecordingFactory.create_with_s3_data(
        s3_client=processing_fixture.s3_client,
        call_center_user=processing_fixture.test_user,
        call=processing_fixture.call,
        audio_content=b"test audio content",
        has_audio=True,
        has_transcript=False,
    )
    with patch(
        "data_pipeline.audio_processing.transcribe_audio",
        return_value=json.loads(transcript_contents),
    ):
        result = run(
            processing_fixture.boto3_session,
            processing_fixture.base_session,
            processing_fixture.settings,
            processing_fixture.deepgram_client,
            processing_fixture.deepgram_options,
            recording.id,
        )

        assert result is True
    # Verify that the interaction was updated with the transcript URL
    recording = processing_fixture.base_session.get(Recording, recording.id)
    processing_fixture.base_session.refresh(recording)

    # Get the actual bucket name and transcript key from the recording
    from data_pipeline.audio_processing import AudioFileS3Uri

    audio_uri = AudioFileS3Uri(recording.audio_url)
    expected_transcript_url = audio_uri.transcript_url

    assert recording.transcript_url == expected_transcript_url

    # Verify that transcript was uploaded to S3
    transcript_s3_path = S3Path(recording.transcript_url)
    transcript = (
        processing_fixture.s3_client.get_object(Bucket=transcript_s3_path.bucket, Key=transcript_s3_path.key)["Body"]
        .read()
        .decode("utf-8")
    )
    assert "going to talk about food allergy" in transcript
    assert isinstance(transcript, str)

    # Verify that the audio file was deleted from S3
    s3_path = S3Path(recording.audio_url)
    assert not s3_path.exists()

    # Verify that the event was written to the event bus
    received_messages = processing_fixture.event_listener.poll()
    assert len(received_messages) == 1
    message = json.loads(received_messages[0]["Body"])
    assert message["detail"]["recording_id"] == recording.id
    assert message["detail"]["event_type"] == "transcribed"

    # Verify CallProcess records were created showing the pipeline progression
    call_processes = processing_fixture.base_session.scalars(
        select(CallProcess).where(CallProcess.recording_id == recording.id)
    ).all()
    assert len(call_processes) == 2  # TRANSCRIPTION_STARTED + TRANSCRIPTION_COMPLETED

    # Check the statuses
    statuses = [cp.process_status for cp in call_processes]
    assert ProcessStatus.TRANSCRIPTION_STARTED in statuses
    assert ProcessStatus.TRANSCRIPTION_COMPLETED in statuses

    # Verify all records have the correct source and recording_id
    for cp in call_processes:
        assert cp.status_source == "test.data-pipeline"
        assert cp.recording_id == recording.id
        assert cp.call_id == recording.call_id

    deepgram_calls = processing_fixture.base_session.scalars(
        select(DeepgramApiCall).where(DeepgramApiCall.recording_id == recording.id)
    ).all()
    assert len(deepgram_calls) == 1
    assert deepgram_calls[0].api_response is None
    assert deepgram_calls[0].success is True
    assert deepgram_calls[0].recording_duration == 2942.528
    assert deepgram_calls[0].confidence == 0.99678016


def test_handler_no_recording(
    processing_fixture,
):
    result = run(
        processing_fixture.boto3_session,
        processing_fixture.base_session,
        processing_fixture.settings,
        processing_fixture.deepgram_client,
        processing_fixture.deepgram_options,
        -1,
    )

    # Should return failure response instead of raising exception
    assert result is False

    received_messages = processing_fixture.event_listener.poll()
    assert len(received_messages) == 0


def test_handler_no_audio_file(processing_fixture):
    recording = RecordingFactory.create_with_s3_data(
        s3_client=processing_fixture.s3_client,
        call_center_user=processing_fixture.test_user,
        call=processing_fixture.call,
        has_audio=False,
        has_transcript=False,
    )

    result = run(
        processing_fixture.boto3_session,
        processing_fixture.base_session,
        processing_fixture.settings,
        processing_fixture.deepgram_client,
        processing_fixture.deepgram_options,
        recording.id,
    )
    assert result is False

    interaction = processing_fixture.base_session.scalars(select(Recording)).all()
    assert len(interaction) == 1
    assert interaction[0].transcript_url is None


def test_handler_invalid_audio_file(processing_fixture):
    invalid_audio_interaction = Recording(
        external_id="test_event",
        audio_url=f"{processing_fixture.s3_bucket}/calls/test/test.wav",
        transcript_url=None,
        call_id=processing_fixture.call.id,
        call_center_user_id=processing_fixture.test_user.id,
    )
    processing_fixture.base_session.add(invalid_audio_interaction)
    processing_fixture.base_session.commit()

    result = run(
        processing_fixture.boto3_session,
        processing_fixture.base_session,
        processing_fixture.settings,
        processing_fixture.deepgram_client,
        processing_fixture.deepgram_options,
        invalid_audio_interaction.id,
    )
    assert result is False

    interaction = processing_fixture.base_session.scalars(select(Recording)).all()
    assert len(interaction) == 1
    assert interaction[0].transcript_url is None

    received_messages = processing_fixture.event_listener.poll()
    assert len(received_messages) == 0


def test_handler_no_transcript(processing_fixture, recording):
    no_audio_interaction = recording
    api_response = {"results": {"channels": [{"alternatives": [{}]}]}}

    with (
        patch(
            "data_pipeline.audio_processing.transcribe_audio",
            return_value=api_response,
        ),
    ):  # Speed up test by skipping retry delays
        result = run(
            processing_fixture.boto3_session,
            processing_fixture.base_session,
            processing_fixture.settings,
            processing_fixture.deepgram_client,
            processing_fixture.deepgram_options,
            no_audio_interaction.id,
        )

        # Should return failure response instead of raising exception
        assert result is False

    # Verify that the audio file still exists in S3
    s3_path = S3Path(no_audio_interaction.audio_url)
    # audio_obj = processing_fixture.s3_client.get_object(Bucket="test-bucket", Key=audio_file_key)
    assert s3_path.exists()

    # Verify CallProcess records were created showing retry attempts + final failure
    call_processes = processing_fixture.base_session.scalars(select(CallProcess)).all()
    assert len(call_processes) == 4  # STARTED + 2 RETRYING + FAILED

    # Check the statuses in order
    statuses = [cp.process_status for cp in call_processes]
    assert ProcessStatus.TRANSCRIPTION_STARTED in statuses
    assert ProcessStatus.TRANSCRIPTION_RETRYING in statuses
    assert ProcessStatus.FAILED in statuses

    # Check the final failure record
    failed_process = [cp for cp in call_processes if cp.process_status == ProcessStatus.FAILED][0]
    assert "No transcript from deepgram" in failed_process.status_text.message

    recording = processing_fixture.base_session.scalars(select(Recording)).all()
    assert len(recording) == 1
    assert recording[0].transcript_url is None

    received_messages = processing_fixture.event_listener.poll()
    assert len(received_messages) == 0

    deepgram_calls = processing_fixture.base_session.scalars(select(DeepgramApiCall)).all()
    assert len(deepgram_calls) == 3
    assert all(deepgram_call.success is False for deepgram_call in deepgram_calls)
    assert all(deepgram_call.api_response == api_response for deepgram_call in deepgram_calls)
    assert all("No transcription results" in deepgram_call.error_message for deepgram_call in deepgram_calls)
    assert all(deepgram_call.confidence is None for deepgram_call in deepgram_calls)


def test_exception_status_text_creation(processing_fixture, recording):
    """Test that PipelineProcessingException creates proper CallProcessMessage objects."""

    # Create a test exception with a specific message
    test_exception_message = "Test pipeline processing error message"
    test_exception = PipelineProcessingException(test_exception_message)

    with (
        # Patch out the main call, so we can explicitly test the exception behavior
        patch(
            "data_pipeline.audio_processing.transcribe_audio",
            side_effect=test_exception,
        ),
    ):
        result = run(
            processing_fixture.boto3_session,
            processing_fixture.base_session,
            processing_fixture.settings,
            processing_fixture.deepgram_client,
            processing_fixture.deepgram_options,
            recording.id,
        )

        # Should return failure response
        assert result is False

        # Find CallProcess objects with ERROR status that should contain exception info
        error_call_processes = processing_fixture.base_session.scalars(
            select(CallProcess)
            .where(CallProcess.recording_id == recording.id)
            .where(CallProcess.process_status == ProcessStatus.ERROR)
        ).all()
        assert len(error_call_processes) == 3  # One per retry attempt

        # Verify that the CallProcessMessage objects contain the exception
        for error_cp in error_call_processes:
            assert error_cp.status_text is not None
            assert error_cp.status_text.message == "Error during transcription API call"
            assert error_cp.status_text.exception is not None
            assert repr(test_exception) in str(error_cp.status_text.exception)

    # Verify no event was published due to the error
    received_messages = processing_fixture.event_listener.poll()
    assert len(received_messages) == 0


def test_exception_recorded_in_call_process(
    processing_fixture,
    recording,
):
    """Test that PipelineProcessingException messages are correctly recorded in the call_process table."""

    # Create a test exception with a specific message
    test_exception_message = "Test pipeline processing error message"
    test_exception = PipelineProcessingException(test_exception_message)

    with (
        patch(
            "data_pipeline.audio_processing.transcribe_audio",
            side_effect=test_exception,
        ),
    ):
        result = run(
            processing_fixture.boto3_session,
            processing_fixture.base_session,
            processing_fixture.settings,
            processing_fixture.deepgram_client,
            processing_fixture.deepgram_options,
            recording.id,
        )

        # Should return failure response
        assert result is False

        # Verify DeepgramApiCall records were created for each attempt
        deepgram_calls = processing_fixture.base_session.scalars(
            select(DeepgramApiCall).where(DeepgramApiCall.recording_id == recording.id)
        ).all()
        assert len(deepgram_calls) == 3
        assert all(not deepgram_call.success for deepgram_call in deepgram_calls)
        assert all(deepgram_call.error_message == test_exception_message for deepgram_call in deepgram_calls)

        # Verify CallProcess records contain the exception message
        call_processes = processing_fixture.base_session.scalars(
            select(CallProcess).where(CallProcess.recording_id == recording.id)
        ).all()
        assert len(call_processes) == 7

        # Find the ERROR status records (created by the specific PipelineProcessingException handler)
        error_processes = [cp for cp in call_processes if cp.process_status == ProcessStatus.ERROR]
        assert len(error_processes) == 3  # One error record per retry attempt

        # Verify the records contain the exception information
        for error_process in error_processes:
            assert error_process.status_text is not None
            assert "Error during transcription API call" in error_process.status_text.message
            assert error_process.status_text.exception is not None

        # Also verify the final FAILED status record exists
        failed_processes = [cp for cp in call_processes if cp.process_status == ProcessStatus.FAILED]
        assert len(failed_processes) == 1
        failed_process = failed_processes[0]
        assert "No results from deepgram" in failed_process.status_text.message

        # Verify no event was published due to the error
        received_messages = processing_fixture.event_listener.poll()
        assert len(received_messages) == 0


def test_exception_after_transcription_call(
    processing_fixture,
    recording,
):
    """Test that PipelineProcessingException messages are correctly recorded when
    exception occurs after transcription."""

    # Create a test exception with a specific message
    test_exception_message = "Test pipeline processing error after transcription"
    test_exception = PipelineProcessingException(test_exception_message)

    with (
        patch(
            "data_pipeline.audio_processing.create_deepgram_record",
            side_effect=test_exception,
        ),
    ):
        result = run(
            processing_fixture.boto3_session,
            processing_fixture.base_session,
            processing_fixture.settings,
            processing_fixture.deepgram_client,
            processing_fixture.deepgram_options,
            recording.id,
        )

        assert result is False

        # Verify CallProcess records were created
        call_processes = processing_fixture.base_session.scalars(
            select(CallProcess).where(CallProcess.recording_id == recording.id)
        ).all()
        assert len(call_processes) == 3

        # Check the statuses
        statuses = [cp.process_status for cp in call_processes]
        assert ProcessStatus.TRANSCRIPTION_STARTED in statuses
        assert ProcessStatus.FAILED in statuses

        # Find the FAILED status record (created by the PipelineProcessingException handler)
        failed_processes = [cp for cp in call_processes if cp.process_status == ProcessStatus.FAILED]
        assert len(failed_processes) == 1
        failed_process = failed_processes[0]

        # Verify the FAILED record contains the exception information
        assert failed_process.status_text is not None
        assert f"Pipeline error occurred: {test_exception_message}" in failed_process.status_text.message
        assert failed_process.status_text.exception is not None
        assert repr(test_exception) in str(failed_process.status_text.exception)

        # Verify all records have the correct source and recording_id
        for cp in call_processes:
            assert cp.status_source == "test.data-pipeline"
            assert cp.recording_id == recording.id
            assert cp.call_id == recording.call_id

        # Verify no DeepgramApiCall records were created since create_deepgram_record failed
        deepgram_calls = processing_fixture.base_session.scalars(
            select(DeepgramApiCall).where(DeepgramApiCall.recording_id == recording.id)
        ).all()
        assert len(deepgram_calls) == 0  # create_deepgram_record was mocked to raise exception

        # Verify no event was published due to the error
        received_messages = processing_fixture.event_listener.poll()
        assert len(received_messages) == 0


def test_handler_skips_processing_when_transcript_exists(
    processing_fixture,
):
    """Test that processing is skipped when recording already has transcript_url."""
    # Verify the test_recording fixture has a transcript_url
    recording = RecordingFactory.create_with_s3_data(
        s3_client=processing_fixture.s3_client,
        call_center_user=processing_fixture.test_user,
        call=processing_fixture.call,
        has_audio=True,
        has_transcript=True,
    )
    recording = processing_fixture.base_session.get(Recording, recording.id)
    assert recording.transcript_url is not None

    # Mock the transcribe_audio function to ensure it's not called
    with patch("data_pipeline.audio_processing.transcribe_audio") as mock_transcribe:
        result = run(
            processing_fixture.boto3_session,
            processing_fixture.base_session,
            processing_fixture.settings,
            processing_fixture.deepgram_client,
            processing_fixture.deepgram_options,
            recording.id,
        )

        # Should return True (success) but without calling transcribe_audio
        assert result is True
        mock_transcribe.assert_not_called()

    # Verify that no new events were written to the event bus
    received_messages = processing_fixture.event_listener.poll()
    assert len(received_messages) == 0


def test_format_speaker_transcript(transcript_contents):
    transcript = format_speaker_transcript(json.loads(transcript_contents))
    assert transcript == [
        {"speaker": "Speaker 1", "text": "Okay, so I mentioned we're going to talk about food allergy."}
    ]


def test_format_speaker_transcript_no_speaker(transcript_contents):
    transcript_json = copy(json.loads(transcript_contents))
    del transcript_json["results"]["channels"][0]["alternatives"][0]["words"][0]["speaker"]
    with pytest.raises(PipelineProcessingException):
        format_speaker_transcript(transcript_json)
