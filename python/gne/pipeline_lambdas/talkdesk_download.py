"""
Downloads files from Talkdesk and saves them in S3 for downstream processing.

]"""

import mimetypes
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Tuple

import boto3
import boto3.session
import requests
from common.database.connect import get_engine_from_secret
from common.database.models import Call, CallCenterUser, CallProcess, CallProcessMessage, Recording
from common.logging import getLogger
from common.messaging.events import EventBridgeMessage
from common.messaging.models import CallProcessStatus, IncomingCall, RecordingProcessEvent
from common.models.db_enum import ProcessStatus
from mypy_boto3_events import EventBridgeClient
from s3pathlib import S3Path, context as s3_context
from sqlalchemy import select
from sqlalchemy.orm import Session

from data_pipeline.shared.batching import BatchTracker
from data_pipeline.shared.errors import PipelineProcessingException
from data_pipeline.shared.talkdesk import TalkdeskSettings, get_bearer_token, get_talkdesk_secret
from data_pipeline.shared.utils import apigateway_response

logger = getLogger(__name__)


class TalkdeskDownloadSettings(TalkdeskSettings):
    max_workers: int = 10
    max_retry_attemps: int = 4
    retry_wait_seconds: int = 30
    allow_duplicate_records: bool = True


@dataclass
class CallDownload:
    call_id: int
    recording_ids: list[int]


def upload_media_to_s3(
    media_url: str,
    token: str,
    call_id: str,
    recording_id: str,
    s3_path: S3Path,
    max_retries: int,
    retry_wait_seconds: int,
) -> str:
    """Downloads a media file and uploads it to S3. Returns the S3 path."""
    headers = {"Authorization": f"Bearer {token}"}
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(media_url, headers=headers, stream=True, timeout=30)
            if response.ok:
                filename = extract_filename(response, recording_id)
                upload_path = s3_path / f"calls/{call_id}/recordings/{recording_id}/{filename}"
                logger.info(f"Uploading to {upload_path}")
                upload_path.write_bytes(response.content)
                return upload_path.uri
            else:
                if attempt == max_retries:
                    logger.error(f"Giving up on recording {recording_id}", extra={"response": response.text})
                    raise PipelineProcessingException(f"Failed to upload recording {recording_id} to S3")
                logger.warning(f"Retry {attempt} for {recording_id} due to HTTP {response.status_code}")

        except requests.RequestException as e:
            if attempt == max_retries:
                logger.error(f"Giving up on recording {recording_id} due to network error", exc_info=e)
                raise PipelineProcessingException(f"Failed to upload recording {recording_id}", inner_exception=e)
            logger.warning(f"Retry {attempt} for {recording_id} due to network error", exc_info=e)
            time.sleep(retry_wait_seconds * attempt)
        except Exception as e:
            logger.error(f"Giving up on recording {recording_id} due to unexpected error. Will not retry.", exc_info=e)
            raise PipelineProcessingException(f"Failed to upload recording {recording_id}", inner_exception=e)

    raise PipelineProcessingException(f"Failed to download recording {recording_id}")


def extract_filename(response, recording_id: str) -> str:
    """
    Extracts a filename with extension from HTTP headers or generates one using content type.
    Defaults to '.mp4' if extension is not supplied and can't be inferred from content type
    """
    disp = response.headers.get("Content-Disposition", "")

    # Try getting file type from extension
    if "filename=" in disp:
        return disp.split("filename=")[-1].strip('";')

    # Try getting it from MIME type if not from extension
    content_type = response.headers.get("Content-Type", "")
    ext = mimetypes.guess_extension(content_type, strict=False) or ".mp4"

    return f"{recording_id}{ext}"


def get_all_recordings(api_url, call_id, token) -> list[dict]:
    all_recordings = []
    current_url: str | None = f"{api_url}/calls/{call_id}/recordings"
    while current_url:
        response = requests.get(current_url, headers={"Authorization": f"Bearer {token}"})
        if not response.ok:
            return []

        data = response.json()
        page_recordings = data.get("_embedded", {}).get("recordings", [])
        all_recordings.extend(page_recordings)

        # Check if there's a next page
        next_link = data.get("_links", {}).get("next")
        current_url = next_link.get("href") if next_link else None

        logger.info(
            f"Fetched {len(page_recordings)} recordings from page "
            f"(total so far: {len(all_recordings)}) for call {call_id}"
        )
    logger.info(f"Found {len(all_recordings)} recordings across all pages for call {call_id}")

    return all_recordings


def run_thread(
    engine,
    call_id: int,
    token: str,
    settings: TalkdeskDownloadSettings,
    eventbridge_client,
) -> bool:
    """Processes a call ID: fetches metadata, downloads media, uploads to S3, and returns recording data."""

    with Session(bind=engine) as session:
        try:
            call = session.scalars(select(Call).where(Call.id == call_id).with_for_update()).one()
            recording_id = None  # define the variable to make sure it's available for the exception

            external_call_id = call.external_id
            s3_path = S3Path(call.call_center.s3_storage_path)

            # Get all recordings for this call, including pagination
            all_recordings = get_all_recordings(settings.api_url, external_call_id, token)
            if not all_recordings:
                logger.warning(f"No recordings found in talkdesk for call {call_id}")
                session.add(
                    CallProcess(
                        call_id=call_id,
                        status_source=settings.event_identifier,
                        process_status=ProcessStatus.FAILED,
                        status_text=CallProcessMessage(message="No downloads found"),
                        created_ts=datetime.now(timezone.utc),
                    )
                )
                session.commit()
                return False

            session.add(
                CallProcess(
                    call_id=call.id,
                    status_source=settings.event_identifier,
                    process_status=ProcessStatus.DOWNLOAD_STARTED,
                    created_ts=datetime.now(timezone.utc),
                )
            )
            session.commit()
            # Process individual recordings
            processed_recordings = []
            for rec in all_recordings:
                recording_id = rec["id"]
                s3_uri = None
                user = None
                created = None
                try:
                    # validate that we don't already have this recording for this call
                    record = session.scalars(
                        select(Recording).where(Recording.external_id == recording_id, Recording.call_id == call.id)
                    ).one_or_none()
                    if record and not settings.allow_duplicate_records:
                        logger.warning(
                            f"Existing record found for record id {recording_id} and call id {call.id}. Skipping"
                        )
                        continue

                    created = datetime.fromisoformat(rec["created_at"].replace("Z", "+00:00"))
                    agents = rec.get("agents", {}).get("channel_2", [])
                    agent = agents[0].get("participant_id") if agents else None

                    # Don't get recording if there is no agent assigned to the recording
                    if not agent:
                        logger.warning(f"No agent assigned to recording {recording_id}, skipping")
                        continue

                    user = session.execute(
                        select(CallCenterUser).where(CallCenterUser.external_id == agent)
                    ).scalar_one_or_none()
                    if not user:
                        logger.warning(
                            f"No user found for agent {agent} assigned to recording {recording_id}, skipping",
                            extra={"recording": rec},
                        )
                        continue

                    recording_url = rec["_links"]["media"]["href"]
                    s3_uri = upload_media_to_s3(
                        recording_url,
                        token,
                        external_call_id,
                        recording_id,
                        s3_path,
                        settings.max_retry_attemps,
                        settings.retry_wait_seconds,
                    )
                    logger.info(f"Successfully uploaded recording {recording_id} to {s3_uri}")

                    recording = Recording(
                        call_id=call.id,
                        external_id=recording_id,
                        call_center_user_id=user.id,
                        start_time=created,
                        audio_url=s3_uri,
                        call_processes=[
                            CallProcess(
                                call_id=call.id,
                                status_source=settings.event_identifier,
                                process_status=ProcessStatus.RECORDING_DOWNLOADED,
                                created_ts=datetime.now(timezone.utc),
                            )
                        ],
                    )
                    session.add(recording)

                    processed_recordings.append(recording)
                except Exception as e:
                    logger.error(f"Error processing recording {recording_id} for call {call_id}", exc_info=e)
                    recording = Recording(
                        call_id=call.id,
                        external_id=recording_id,
                        call_center_user_id=user.id if user else None,
                        start_time=created,
                        audio_url=s3_uri,
                        call_processes=[
                            CallProcess(
                                call_id=call.id,
                                status_source=settings.event_identifier,
                                process_status=ProcessStatus.FAILED,
                                created_ts=datetime.now(timezone.utc),
                            )
                        ],
                    )
                    session.add(recording)
                session.commit()

            # If no recordings, bail out early
            if len(processed_recordings) == 0:
                logger.error(f"Unable to process {len(all_recordings)} recordings for call {call_id}")
                session.add(
                    CallProcess(
                        call_id=call_id,
                        status_source=settings.event_identifier,
                        process_status=ProcessStatus.FAILED,
                        status_text=CallProcessMessage(message="No recordings found"),
                        created_ts=datetime.now(timezone.utc),
                    )
                )
                session.commit()
                return False

            # Update the call record
            session.add(
                CallProcess(
                    call_id=call.id,
                    status_source=settings.event_identifier,
                    process_status=ProcessStatus.DOWNLOADED,
                    created_ts=datetime.now(timezone.utc),
                )
            )
            session.commit()

            # update the call with the earliest start time found
            earliest_recording = session.scalars(
                select(Recording).where(Recording.call_id == call_id).order_by(Recording.start_time.asc()).limit(1)
            ).one_or_none()

            if earliest_recording:
                call.start_time = earliest_recording.start_time
                logger.info(f"Updated call {call_id} with start time {call.start_time}")
            session.commit()

            # Publish all the recordings
            recording_ids = [recording.id for recording in processed_recordings]
            for recording_id in recording_ids:
                next_event_detail = RecordingProcessEvent(
                    event_type=CallProcessStatus.DOWNLOADED, recording_id=recording_id
                )
                next_message = EventBridgeMessage(
                    source=settings.event_identifier,
                    detail=next_event_detail,
                    event_bus_name=settings.event_bus_name,
                )
                eventbridge_client.put_events(Entries=[next_message.to_bridge_event()])
                logger.info(
                    f"Published next event for call {id}",
                    extra={"message": next_message.to_bridge_event()},
                )

            logger.info(f"Published {len(recording_ids)} events for call {call_id}")
            return True

        except eventbridge_client.exceptions.ClientError as e:
            logger.error("Failed to publish next event", exc_info=e)
            session.add(
                CallProcess(
                    call_id=call_id,
                    recording_id=recording_id if recording_id else None,
                    status_source=settings.event_identifier,
                    process_status=ProcessStatus.FAILED,
                    status_text=CallProcessMessage(
                        message="Failed to publish next event",
                        exception=e,
                    ),
                    created_ts=datetime.now(timezone.utc),
                )
            )
        except Exception as e:
            logger.error("Unexpected error processing calls", exc_info=e)
            session.add(
                CallProcess(
                    call_id=call_id,
                    recording_id=recording_id if recording_id else None,
                    status_source=settings.event_identifier,
                    process_status=ProcessStatus.FAILED,
                    status_text=CallProcessMessage(
                        message="Unexpected error processing calls",
                        exception=e,
                    ),
                    created_ts=datetime.now(timezone.utc),
                )
            )
        finally:
            session.commit()

    return False


def run_all(
    tracker: BatchTracker,
    eventbridge_client: EventBridgeClient,
    db_session: Session,
    settings: TalkdeskDownloadSettings,
    token: str,
):
    with ThreadPoolExecutor(max_workers=settings.max_workers) as executor:
        # multithread the downloads
        call_futures: Dict[Future[bool], Tuple[EventBridgeMessage, int]] = {}
        for parsed_event in tracker.events:
            message: IncomingCall = parsed_event.detail
            call_id: int = message.event_id

            if message.event_type != CallProcessStatus.CREATED:
                tracker.mark_event(parsed_event, False)
                logger.warning(f"Received event {parsed_event}, but it wasn't in the CREATED state. Skipping")
                continue

            call_futures |= {
                executor.submit(
                    run_thread,
                    db_session.bind,
                    call_id,
                    token,
                    settings,
                    eventbridge_client,
                ): (parsed_event, call_id)
            }

        # process the results
        for future in call_futures:
            parsed_event, call_id = call_futures[future]
            status = future.result()
            tracker.mark_event(parsed_event, status)

        db_session.commit()
    return tracker.generate_response()


def handler(event: dict, context: dict) -> dict:
    """
    Lambda handler for downloading Talkdesk recordings.

    Expected event structure:
    {
        "detail": {
            "version": "1.0",
            "event_type": "created",
            "event_ids": [1, 2, 3]
        }
    }

    """
    logger.info("Processing talkdesk download event", extra={"event": event})
    start_time = datetime.now(timezone.utc)
    logger.info("Started processing", extra={"start_time": start_time})
    settings = TalkdeskDownloadSettings()

    try:
        tracker = BatchTracker(event, IncomingCall)
        logger.info(f"Found {len(tracker.events)} events")
    except Exception as e:
        logger.error("Unable to process lambda event", exc_info=e, extra={"message": event})
        return apigateway_response(status_code=500, body={"message": "Unable to process lambda event", "event": event})

    boto3_session = boto3.session.Session()
    eventbridge_client = boto3_session.client("events")
    s3_context.attach_boto_session(boto3_session)

    try:
        talkdesk_secret = get_talkdesk_secret(settings, boto3_session)
        token = get_bearer_token(talkdesk_secret)

        with Session(bind=get_engine_from_secret(settings.rds_secret_name, boto3_session)) as db_session:
            response = run_all(
                tracker=tracker,
                eventbridge_client=eventbridge_client,
                db_session=db_session,
                settings=settings,
                token=token,
            )
            end_time = datetime.now(timezone.utc)
            logger.info(
                "Finished processing",
                extra={"end_time": end_time, "duration": end_time - start_time, "event_count": len(tracker.events)},
            )

            return response

    except Exception as e:
        logger.error("Error processing talkdesk download request", exc_info=e)
        return apigateway_response(500, {"error": "Internal server error"})


"""
Sample test event to test the function in lambda:
{
    "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
    "detail-type": "IncomingCalls",
    "source": "aws.events",
    "account": "123456789012",
    "time": "1970-01-01T00:00:00Z",
    "region": "us-east-1",
    "resources": ["arn:aws:events:us-east-1:123456789012:rule/ExampleRule"],
    "detail": {
        "version": "1.0",
        "event_type": "created",
        "event_ids": [16, 17, 18],
    },
}
"""
