import json
import pathlib
from datetime import datetime, timezone
from time import sleep, time
from urllib.parse import urlparse

import boto3
import httpx
from common.database.connect import get_engine_from_secret
from common.database.models import CallProcess, CallProcessMessage, DeepgramApiCall, Recording
from common.logging import getLogger
from common.messaging.events import EventBridgeMessage
from common.messaging.models import CallProcessStatus, RecordingProcessEvent
from common.models.db_enum import ProcessStatus
from deepgram import DeepgramClient, DeepgramClientOptions, PrerecordedOptions
from sqlalchemy import select
from sqlalchemy.orm import Session

from data_pipeline.shared.batching import BatchTracker
from data_pipeline.shared.errors import PipelineProcessingException
from data_pipeline.shared.settings import PipelineSettings
from data_pipeline.shared.utils import apigateway_response

logger = getLogger(__name__)


class DeepgramSettings(PipelineSettings):
    deepgram_api_secret_name: str
    max_retries: int = 3
    retry_wait_seconds: int = 30

    def _load_secrets(self):
        if not hasattr(self, "_secrets"):
            secrets_client = boto3.client("secretsmanager")
            self._secrets = get_secret(self.deepgram_api_secret_name, secrets_client)

    @property
    def deepgram_api_key(self) -> str:
        """
        Retrieves the Deepgram API key from AWS Secrets Manager.
        """
        self._load_secrets()
        result = self._secrets.get("API_KEY")
        if result is None:
            raise ValueError(f"Deepgram API key not found in secret {self.deepgram_api_secret_name}.")
        return result

    @property
    def deepgram_api_url(self) -> str:
        """
        Returns the Deepgram API URL.
        """
        self._load_secrets()

        result = self._secrets.get("HOSTNAME")
        if result is None:
            raise ValueError(f"Deepgram API URL not found in secret {self.deepgram_api_secret_name}.")
        return result


class AudioFileS3Uri:
    """
    Represents an S3 URI for an audio file.
    """

    def __init__(self, url: str):
        self._parsed_url = urlparse(url, allow_fragments=False)
        if self._parsed_url.scheme.lower() != "s3":
            raise ValueError(f"Invalid S3 URL: {url}. Expected scheme 's3'.")

    @property
    def bucket_name(self) -> str:
        """
        Returns the S3 bucket name from the parsed URL.
        """
        return self._parsed_url.netloc

    @property
    def audio_key(self) -> str:
        """
        Returns the S3 key (path) for the audio file, stripping leading slashes.
        """
        key = self._parsed_url.path.lstrip("/")
        if not key.startswith("calls/"):
            raise ValueError(f"Invalid audio file key: {key}. Expected to start with 'calls/'.")
        if not key.lower().endswith(".mp3"):
            raise ValueError(f"Invalid audio file key: {key}. Expected a .mp3 file.")
        return key

    @property
    def transcript_url(self) -> str:
        """
        Returns the full S3 URL for the transcript file.
        """
        return f"s3://{self.bucket_name}/{self.transcript_key}"

    @property
    def transcript_key(self) -> str:
        """
        Builds the S3 key for the transcript file based on the audio file's S3 key.
        The transcript file is stored in the 'transcripts/' directory with a '.txt' extension.
        """
        output_key = self.audio_key.replace("calls/", "transcripts/", 1)
        output_key = output_key[:-4] + ".txt"
        return output_key


def get_secret(secret_name: str, secrets_client: boto3.client) -> dict:
    """
    Retrieve a secret from AWS Secrets Manager.
    """
    secret_value = secrets_client.get_secret_value(SecretId=secret_name)["SecretString"]
    return json.loads(secret_value) if isinstance(secret_value, str) else secret_value


def transcribe_audio(
    deepgram_client: DeepgramClient, deepgram_options: PrerecordedOptions, audio_stream: bytes
) -> dict:
    """
    Transcribe audio data using the Deepgram API.

    Args:
        audio_stream (bytes): The audio data to transcribe.

    Returns:
        str: The transcribed text of the audio data.
    """
    source = {"buffer": audio_stream}
    try:
        response = deepgram_client.listen.rest.v("1").transcribe_file(
            source,
            deepgram_options,
            timeout=httpx.Timeout(
                300.0,
                connect=10.0,
            ),
        )
        return response.to_dict()
    except Exception as e:
        logger.error(f"Error during transcription: {e}")

        raise PipelineProcessingException("Failed deepgram call", inner_exception=e)


def format_speaker_transcript(transcription: dict) -> list[dict] | None:
    """
    Format Deepgram transcription into a list of dicts by speaker.

    Requires diarization to be enabled in Deepgram settings because two assumptions are made:
    1. A speaker always exists
    2. The speaker is an integer

    https://developers.deepgram.com/reference/speech-to-text-api/listen#request.query.diarize.diarize

    Args:
        transcription(dict): The transcription from Deepgram. Requires diarization to be enabled in Deepgram settings.

    Raises:
        PipelineProcessingException: If the speaker is not found in the Deepgram response.

    Returns:
        If a transcript is found, returns a list of dicts, where each dict contains a speaker and their text.
        If no transcript is found, returns None.

    Example: [{"speaker": "Speaker 1", "text": "text 1..."}, {"speaker": "Speaker 2", "text": "text 2..."}, ...]
    """
    results = transcription.get("results")
    if not results:
        return None

    speaker_transcripts = []
    current_speaker = None
    current_segment_text: list[str] = []

    for channel in results.get("channels", []):
        for alternative in channel.get("alternatives", []):
            words = alternative.get("words", [])
            for word_info in words:
                try:
                    speaker = str(word_info["speaker"])
                except KeyError:
                    raise PipelineProcessingException(
                        "Speaker not found in Deepgram response. Ensure diarization is enabled in Deepgram settings."
                    )
                word = word_info.get("punctuated_word") or word_info.get("word", "")
                if current_speaker != speaker:
                    if current_speaker is not None and current_segment_text:
                        current_speaker_label = f"Speaker {int(current_speaker) + 1}"
                        speaker_transcripts.append(
                            {"speaker": current_speaker_label, "text": " ".join(current_segment_text)}
                        )
                    current_speaker = speaker
                    current_segment_text = []
                current_segment_text.append(word)
    if current_speaker and current_segment_text:
        current_speaker_label = f"Speaker {int(current_speaker) + 1}"
        speaker_transcripts.append({"speaker": current_speaker_label, "text": " ".join(current_segment_text)})

    return speaker_transcripts


def create_deepgram_record(
    start_time,
    recording_id: int,
    transcript_results: dict | None,
    success: bool,
    *,
    error_message=None,
    transcript=None,
) -> DeepgramApiCall:
    end_time = time()
    elapsed_time = end_time - start_time

    deepgram_log = DeepgramApiCall(
        recording_id=recording_id,
        recording_duration=transcript_results.get("metadata", {}).get("duration") if transcript_results else None,
        length=len(transcript) if transcript else None,
        elapsed_time=elapsed_time,
        # Only store the response if we didn't get a transcript, to avoid logging PHI
        api_response=transcript_results if not transcript else None,
        error_message=error_message,
        success=success,
    )
    try:
        deepgram_log.confidence = (
            transcript_results.get("results", {}).get("channels", {})[0].get("alternatives", {})[0].get("confidence")
            if transcript_results
            else None
        )

    except (KeyError, IndexError):
        deepgram_log.confidence = None

    return deepgram_log


def run(
    boto3_session,
    db_session: Session,
    settings: DeepgramSettings,
    deepgram_options: PrerecordedOptions,
    deepgram_client: DeepgramClient,
    event_id: str,
) -> bool:
    s3_client = boto3_session.client("s3")
    events_client = boto3_session.client("events")
    try:
        recording = db_session.scalars(select(Recording).where(Recording.id == event_id)).one_or_none()

        if recording and recording.transcript_url:
            logger.info(
                "Transcript already processed. Skipping.",
                extra={
                    "recording": {
                        "id": recording.id,
                        "external_id": recording.external_id,
                        "transcript_url": recording.transcript_url,
                    }
                },
            )
            return True

        if not recording:
            raise PipelineProcessingException(f"Unable to locate interaction for event id {event_id}")

        audio_file = AudioFileS3Uri(recording.audio_url)
        audio_data = s3_client.get_object(
            Bucket=audio_file.bucket_name,
            Key=audio_file.audio_key,
        )["Body"].read()

        retries = settings.max_retries
        transcript = None  # Initialize for exception handling
        transcript_results = None

        for attempt in range(1, retries + 1):
            if attempt == 1:
                db_session.add(
                    CallProcess(
                        call_id=recording.call_id,
                        recording_id=recording.id,
                        status_source=settings.event_identifier,
                        process_status=ProcessStatus.TRANSCRIPTION_STARTED,
                        created_ts=datetime.now(timezone.utc),
                    )
                )
            else:
                db_session.add(
                    CallProcess(
                        call_id=recording.call_id,
                        recording_id=recording.id,
                        status_source=settings.event_identifier,
                        process_status=ProcessStatus.TRANSCRIPTION_RETRYING,
                        status_text=CallProcessMessage(message=f"attempt #{attempt}"),
                        created_ts=datetime.now(timezone.utc),
                    )
                )
            db_session.commit()

            start_time = time()
            try:
                transcript_results = transcribe_audio(
                    deepgram_client,
                    deepgram_options,
                    audio_data,
                )
            except PipelineProcessingException as e:
                # Handle transcription API exceptions specifically
                logger.warning(
                    "Error during transcription API call",
                    exc_info=e,
                    extra={"attempt": attempt, "retries": retries},
                )
                if recording:
                    db_session.add(
                        CallProcess(
                            call_id=recording.call_id,
                            recording_id=recording.id,
                            status_source=settings.event_identifier,
                            process_status=ProcessStatus.ERROR,
                            status_text=CallProcessMessage(message="Error during transcription API call", exception=e),
                            created_ts=datetime.now(timezone.utc),
                        )
                    )
                db_session.add(
                    create_deepgram_record(
                        start_time,
                        recording.id,
                        None,  # No transcript results due to API failure
                        success=False,
                        error_message=e.message,
                    )
                )
                db_session.commit()
                # Continue to next retry attempt
                continue

            transcript = format_speaker_transcript(transcript_results)
            recording_duration = transcript_results.get("metadata", {}).get("duration")

            if transcript:
                db_session.add(
                    create_deepgram_record(
                        start_time,
                        recording.id,
                        transcript_results,
                        success=True,
                        transcript=json.dumps(transcript),
                    )
                )
                db_session.commit()
                break

            db_session.add(
                create_deepgram_record(
                    start_time,
                    recording.id,
                    transcript_results,
                    success=False,
                    error_message="No transcription results found in Deepgram response.",
                )
            )

            db_session.commit()

            # Wait before retry
            wait_time = attempt * settings.retry_wait_seconds
            logger.info(f"Retrying transcription in {wait_time} seconds.")
            sleep(wait_time)

        # If, after the end of retries, still no transcript, bail out
        if not transcript:
            logger.error(
                "No transcript found from deepgram",
                extra={"response": transcript_results, "recording": {"external_id": recording.external_id}},
            )
            if transcript_results:
                message = f"No transcript from deepgram. Recording: {recording.external_id}"
            else:
                message = f"No results from deepgram. Recording: {recording.external_id}"
            db_session.add(
                CallProcess(
                    call_id=recording.call_id,
                    recording_id=recording.id if recording else None,
                    status_source=settings.event_identifier,
                    process_status=ProcessStatus.FAILED,
                    status_text=CallProcessMessage(message=message),
                    created_ts=datetime.now(timezone.utc),
                )
            )

            return False

        # process the transcript
        s3_client.put_object(
            Bucket=audio_file.bucket_name,
            Key=audio_file.transcript_key,
            Body=json.dumps(transcript),
        )
        recording.transcript_url = audio_file.transcript_url
        recording.duration = recording_duration
        db_session.add(recording)
        db_session.commit()

        s3_client.delete_object(
            Bucket=audio_file.bucket_name,
            Key=audio_file.audio_key,
        )
        logger.info(f"Deleted audio file from S3: s3://{audio_file.bucket_name}/{audio_file.audio_key}")

        # record completion
        db_session.add(
            CallProcess(
                call_id=recording.call_id,
                recording_id=recording.id,
                status_source=settings.event_identifier,
                process_status=ProcessStatus.TRANSCRIPTION_COMPLETED,
                created_ts=datetime.now(timezone.utc),
            )
        )
        db_session.commit()

        detail = RecordingProcessEvent(recording_id=event_id, event_type=CallProcessStatus.TRANSCRIBED)
        event_entry = EventBridgeMessage(
            source=settings.event_identifier,
            detail=detail,
            event_bus_name=settings.event_bus_name,
        ).to_bridge_event()

        response = events_client.put_events(Entries=[event_entry])
        if response.get("FailedEntryCount", 1):
            logger.error("Unable to publish message to event bridge", extra={"message", event_entry})
            return False
    except ValueError as e:
        logger.error("Error processing S3 key.", exc_info=e)
        if recording:
            db_session.add(
                CallProcess(
                    call_id=recording.call_id,
                    recording_id=recording.id,
                    status_source=settings.event_identifier,
                    process_status=ProcessStatus.FAILED,
                    status_text=CallProcessMessage(
                        message=f"Error processing s3 key {recording.audio_url}", exception=e
                    ),
                    created_ts=datetime.now(timezone.utc),
                )
            )
            return False

    except PipelineProcessingException as e:
        logger.error(f"Pipeline error occurred: {e.message}", exc_info=e)
        if recording:
            db_session.add(
                CallProcess(
                    call_id=recording.call_id,
                    recording_id=recording.id,
                    status_source=settings.event_identifier,
                    process_status=ProcessStatus.FAILED,
                    status_text=CallProcessMessage(message=f"Pipeline error occurred: {e.message}", exception=e),
                    created_ts=datetime.now(timezone.utc),
                )
            )
        return False
    except Exception as e:
        logger.error("Unexpected error occurred", exc_info=e)
        if recording:
            db_session.add(
                CallProcess(
                    call_id=recording.call_id,
                    recording_id=recording.id,
                    status_source=settings.event_identifier,
                    process_status=ProcessStatus.FAILED,
                    created_ts=datetime.now(timezone.utc),
                )
            )
        return False

    finally:
        db_session.commit()

    return True


def run_all(
    tracker: BatchTracker,
    boto3_session: boto3.session.Session,
    db_session: Session,
    settings: DeepgramSettings,
    deepgram_options: PrerecordedOptions,
    deepgram_client: DeepgramClient,
):
    for parsed_event in tracker.events:
        message = parsed_event.detail
        event_id = message.recording_id

        if message.event_type != CallProcessStatus.DOWNLOADED:
            tracker.mark_event(parsed_event, False)
            logger.warning(f"Received event {parsed_event}, but it wasn't in the DOWNLOADED state. Skipping")
            continue

        logger.info(f"Transcribing audio for {event_id}", extra=message)

        result = run(boto3_session, db_session, settings, deepgram_options, deepgram_client, event_id)
        tracker.mark_event(parsed_event, result)


def handler(event, context):
    """AWS Lambda handler: triggered via EventBridge on S3 upload."""

    logger.info(f"Processing detection event {event=}")
    start_time = datetime.now(timezone.utc)
    logger.info("Started processing", extra={"start_time": start_time})

    boto3_session = boto3.session.Session()

    settings = DeepgramSettings()

    with open(pathlib.Path(__file__).parent.absolute() / "transcription_keywords.txt", "r") as f:
        deepgram_keywords = [line.strip() for line in f.read().splitlines() if line.strip()]

    deepgram_options = PrerecordedOptions(
        model="nova-2-medical",
        smart_format=True,
        diarize=True,
        multichannel=False,
        keywords=deepgram_keywords,
    )
    deepgram_client = DeepgramClient(
        config=DeepgramClientOptions(
            api_key=settings.deepgram_api_key,
            url=settings.deepgram_api_url,
        )
    )

    try:
        tracker = BatchTracker(event, RecordingProcessEvent)
        logger.info(f"Found {len(tracker.events)} events")
    except Exception as e:
        logger.error("Unable to process lambda event", exc_info=e, extra={"message": event})
        return apigateway_response(status_code=500, body={"message": "Unable to process lambda event", "event": event})

    boto3_session = boto3.session.Session()
    with Session(bind=get_engine_from_secret(settings.rds_secret_name, boto3_session)) as db_session:
        run_all(tracker, boto3_session, db_session, settings, deepgram_options, deepgram_client)

    end_time = datetime.now(timezone.utc)
    logger.info(
        "Finished processing",
        extra={"end_time": end_time, "duration": end_time - start_time, "event_count": len(tracker.events)},
    )
    return tracker.generate_response()


"""
Sample test event to test the function in lambda:
{
    "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
    "detail-type": "Scheduled Event",
    "source": "aws.events",
    "account": "123456789012",
    "time": "1970-01-01T00:00:00Z",
    "region": "us-east-1",
    "resources": ["arn:aws:events:us-east-1:123456789012:rule/ExampleRule"],
    "detail": {
        "version": "0.1",
        "event_type": "created",
        "recording_id": 19,
    },
}
"""
