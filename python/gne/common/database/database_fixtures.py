import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import factory
from common.database.connect import get_engine
from common.database.models import (
    Call,
    CallCenter,
    CallCenterSsoGroupMap,
    CallCenterUser,
    Patient,
    PotentialReportableEvent,
    Product,
    Recording,
)
from factory import Faker, LazyAttribute, LazyFunction, SubFactory
from factory.alchemy import SQLAlchemyModelFactory
from factory.fuzzy import FuzzyChoice, FuzzyDateTime
from s3pathlib import S3Path
from sqlalchemy import inspect, text
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy_utils import create_database, database_exists

test_session = scoped_session(sessionmaker())

USER_ROLE = "GLOORG_Genentech-Users"
SUPERVISOR_ROLE = "Deepsense_dev_aria_ASO_supervisors"


########################################
# Engine Fixtures
########################################
def init_engine(settings):
    engine = get_engine(settings)
    if not database_exists(engine.url):
        create_database(engine.url)
    return engine


def drop_database(engine):
    try:
        inspector = inspect(engine)
        schemas = [schema for schema in inspector.get_schema_names() if schema not in ["information_schema"]]
        with engine.connect() as conn:
            # First terminate all other connections to the database
            conn.execute(
                text(
                    """
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = current_database()
                AND pid <> pg_backend_pid()
            """
                )
            )
            conn.commit()

            # Drop schemas and their types
            for schema in schemas:
                try:
                    # First drop all tables in the schema
                    conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
                    conn.commit()

                    # Recreate the schema
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                    conn.commit()
                except Exception as e:
                    print(f"Error handling schema {schema}: {e}")
                    continue
    except Exception as e:
        print(f"Error during database cleanup: {e}")


########################################
# Model Factories
########################################
def get_test_session():
    """Get the current test session."""
    return test_session()


class BaseFactory(SQLAlchemyModelFactory):
    """Base factory with common configuration."""

    class Meta:
        abstract = True
        sqlalchemy_session_persistence = "commit"
        sqlalchemy_session_factory = get_test_session


def generate_call_duration():
    """Generate realistic call duration."""
    call_type = random.choice(["short", "medium", "long"])
    if call_type == "short":
        return random.uniform(15.0, 180.0)
    elif call_type == "medium":
        return random.uniform(180.0, 900.0)
    else:
        return random.uniform(900.0, 3600.0)


def generate_evidence():
    """Generate evidence using Factory Boy Faker approach."""
    from faker import Faker as FakeLib

    fake = FakeLib()

    num_evidence = random.randint(1, 3)
    evidence = []

    for _ in range(num_evidence):
        reason = fake.text(max_nb_chars=200)
        num_quotes = random.randint(1, 4)
        quotes = [fake.text(max_nb_chars=100) for _ in range(num_quotes)]

        evidence.append({"reason": reason, "supporting_quotes": quotes})

    return evidence


########################################
# Core Database factories
########################################
class CallCenterFactory(BaseFactory):
    """Factory for CallCenter model."""

    class Meta:
        model = CallCenter

    external_id = LazyAttribute(lambda o: f"ctr_{random.randint(1000, 9999)}")
    name = Faker("company")
    s3_storage_path = LazyAttribute(lambda o: f"s3://test-bucket-{o.external_id.lower()}")


class CallCenterUserFactory(BaseFactory):
    """Factory for CallCenterUser model."""

    class Meta:
        model = CallCenterUser

    external_id = Faker("user_name")
    display_name = Faker("name")
    email = Faker("email")
    call_center = SubFactory(CallCenterFactory)
    active = True


class CallFactory(BaseFactory):
    """Factory for Call model."""

    class Meta:
        model = Call

    external_id = Faker("uuid4")
    call_center = SubFactory(CallCenterFactory)
    start_time = FuzzyDateTime(
        start_dt=datetime.now(timezone.utc) - timedelta(days=30),
        end_dt=datetime.now(timezone.utc) + timedelta(hours=1),
    )

    @LazyAttribute
    def end_time(self):
        if self.start_time:
            duration = random.uniform(60, 3600)  # 1 minute to 1 hour
            return self.start_time + timedelta(seconds=duration)
        return None


class RecordingFactory(BaseFactory):
    """Factory for Recording model."""

    class Meta:
        model = Recording
        exclude = ("bucket_key", "bucket_name")

    external_id = Faker("uuid4")
    call_center_user = SubFactory(CallCenterUserFactory)
    call = SubFactory(CallFactory)

    # Hidden parameters that won't be passed to the model
    bucket_key = LazyAttribute(lambda o: f"{o.external_id}")
    bucket_name = LazyAttribute(lambda o: o.call.call_center.s3_storage_path.replace("s3://", "").split("/")[0])

    # Use the hidden parameters to build the actual model fields
    audio_url = LazyAttribute(lambda o: f"s3://{o.bucket_name}/calls/{o.bucket_key}.mp3" if o.has_audio else None)
    transcript_url = LazyAttribute(
        lambda o: f"s3://{o.bucket_name}/transcript/{o.bucket_key}.txt" if o.has_transcript else None
    )
    start_time = LazyAttribute(lambda o: o.call.start_time)
    duration = LazyFunction(lambda: generate_call_duration())

    class Params:
        has_audio = True
        has_transcript = True
        audio_content = None
        transcript_content = None

    @classmethod
    def create_with_s3_data(cls, s3_client, audio_content=None, transcript_content=None, **kwargs):
        """
        Create a Recording instance and set up associated S3 data.

        Args:
            s3_client: Boto3 S3 client for creating buckets and uploading content
            audio_content: Optional bytes for audio file content
            transcript_content: Optional list of transcript data
            **kwargs: Additional parameters to pass to the factory

        Returns:
            Recording: The created Recording instance with S3 data uploaded
        """
        # Create the recording instance first
        recording = cls.create(**kwargs)

        # Set up S3 data if client is provided
        if s3_client:
            cls._setup_s3_data(
                recording=recording,
                s3_client=s3_client,
                audio_content=audio_content,
                transcript_content=transcript_content,
            )

        return recording

    @classmethod
    def _setup_s3_data(cls, recording, s3_client, audio_content=None, transcript_content=None):
        """
        Private helper method to set up S3 data for a recording.

        Args:
            recording: The Recording instance
            s3_client: Boto3 S3 client
            audio_content: Optional bytes for audio file content
            transcript_content: Optional list of transcript data
        """
        # Determine which URL to use for bucket extraction
        url = recording.audio_url or recording.transcript_url
        if not url:
            return
        url_path = S3Path(url)

        # Extract bucket name
        bucket_name = url_path.bucket

        # Check if bucket exists, create if it doesn't
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except Exception:
            # Create bucket with appropriate region configuration
            try:
                s3_client.create_bucket(
                    Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
                )
            except Exception:
                # For us-east-1, don't specify LocationConstraint
                s3_client.create_bucket(Bucket=bucket_name)

        # Upload transcript data if transcript URL exists
        if recording.transcript_url:
            s3_path = S3Path(recording.transcript_url)
            transcript_data = transcript_content or [{"speaker": "Speaker 1", "text": "hello world"}]
            key = s3_path.key
            s3_client.put_object(Bucket=s3_path.bucket, Key=key, Body=json.dumps(transcript_data).encode("utf-8"))

        # Upload audio data if audio URL exists
        if recording.audio_url:
            s3_path = S3Path(recording.audio_url)
            key = s3_path.key
            audio_data = audio_content or b""
            s3_client.put_object(Bucket=s3_path.bucket, Key=key, Body=audio_data)


class PatientFactory(BaseFactory):
    """Factory for Patient model."""

    class Meta:
        model = Patient

    recording_id = SubFactory(RecordingFactory)
    patient_name = Faker("name")
    date_of_birth = FuzzyDateTime(
        start_dt=datetime(1930, 1, 1).replace(tzinfo=timezone.utc),
        end_dt=datetime(2010, 12, 31).replace(tzinfo=timezone.utc),
    )
    gender = FuzzyChoice(["M", "F", "O"])
    notes = Faker("text", max_nb_chars=500)


class ProductFactory(BaseFactory):
    """Factory for Product model."""

    class Meta:
        model = Product

    name = FuzzyChoice(["Test Drug A", "Test Drug B", "Test Drug C", "Medication X", "Medication Y", "Medication Z"])
    recording_id = SubFactory(RecordingFactory)
    patient_id = SubFactory(PatientFactory)


class PotentialReportableEventFactory(BaseFactory):
    """Factory for PotentialReportableEvent model."""

    class Meta:
        model = PotentialReportableEvent

    recording = SubFactory(RecordingFactory)
    confidence_score = LazyFunction(lambda: random.uniform(0.0, 5.0))
    evidence = LazyFunction(lambda: generate_evidence())
    confirmed_ae = None  # Default to unconfirmed
    reported_status = None

    # Reporter details
    reporter_name = Faker("name")
    reporter_type = FuzzyChoice(["HCP", "Patient", "Caregiver"])
    reporter_contact = Faker("email")
    permission_to_follow_up = FuzzyChoice([True, False])

    # AE response field
    ae_response = LazyFunction(
        lambda: {
            "processed_at": datetime.now().isoformat(),
            "confidence": random.uniform(0.0, 1.0),
            "model_version": "v1.0",
        }
    )

    @classmethod
    def create_confirmed(cls, **kwargs):
        """Create a confirmed PRE."""

        args = kwargs | dict(confirmed_ae=True)
        return cls.create(**args)

    @classmethod
    def create_rejected(cls, **kwargs):
        """Create a rejected PRE."""
        args = kwargs | dict(
            confirmed_ae=False, rejected_ae_reason="OTHER", other_rejected_reason="Test rejection reason"
        )
        return cls.create(**args)


class CallCenterSsoGroupMapFactory(BaseFactory):
    """Factory for CallCenterSsoGroupMap model."""

    class Meta:
        model = CallCenterSsoGroupMap

    call_center = SubFactory(CallCenterFactory)
    sso_group_name = FuzzyChoice([USER_ROLE, SUPERVISOR_ROLE, "Test_Group"])
    is_supervisor = FuzzyChoice([True, False, None])


########################################
# Complex Scenario fixtures
########################################
@dataclass
class CallCenterSetup:
    """
    Dataclass to hold a fully configured call center, populating the database with
    * Call Center
    * Agent and Supervisor groups
    * Agent user
    * Supervisor user

    This class provides convenience methods to generate recordings and PREs based on
    the data it holds
    """

    call_center: CallCenter
    group_map: list[CallCenterSsoGroupMap]
    agent: CallCenterUser
    supervisor: CallCenterUser

    def add_recording(self, boto_session=None, audio_content=None, transcript_content=None, **kwargs):
        """Add a recording to this call center setup."""
        user = self.agent

        # Create a call for this call center
        if "call" not in kwargs:
            call = CallFactory.create(call_center=self.call_center)
            kwargs["call"] = call
        else:
            call = kwargs["call"]

        if boto_session:
            s3_client = boto_session.client("s3")
            # Use the new factory method to create recording with S3 data
            recording = RecordingFactory.create_with_s3_data(
                s3_client=s3_client,
                audio_content=audio_content,
                transcript_content=transcript_content,
                call_center_user=user,
                call=call,
            )
        else:
            # Create recording without S3 data setup
            recording = RecordingFactory.create(call_center_user=user, call=call)

        return recording

    def add_pre(self, boto_session=None, with_patient=False, pre_params: dict = {}):
        """
        Add a PRE to the setup
        """
        recording = self.add_recording(boto_session=boto_session)

        if with_patient:
            patient = PatientFactory.create(
                recording_id=recording.id, patient_name="Test Patient", date_of_birth=datetime(2000, 1, 1)
            )
            ProductFactory.create(recording_id=recording.id, patient_id=patient.id, name="Test Drug")

        return PotentialReportableEventFactory.create(
            recording=recording,
            **pre_params,
        )


class CallCenterSetupFactory(factory.Factory):
    class Meta:
        model = CallCenterSetup

    call_center = SubFactory(CallCenterFactory)

    @LazyAttribute
    def group_map(self):
        return [
            # Using the factories to enforce the database record creation
            CallCenterSsoGroupMapFactory.create(
                call_center=self.call_center,
                sso_group_name=USER_ROLE,
                is_supervisor=False,
            ),
            CallCenterSsoGroupMapFactory.create(
                call_center=self.call_center, sso_group_name=SUPERVISOR_ROLE, is_supervisor=True
            ),
        ]

    agent = SubFactory(
        CallCenterUserFactory, call_center=factory.SelfAttribute("..call_center"), external_id="agent_user", active=True
    )

    supervisor = SubFactory(
        CallCenterUserFactory,
        call_center=factory.SelfAttribute("..call_center"),
        external_id="supervisor_user",
        active=True,
    )

    @classmethod
    def create_with_s3_setup(cls, s3_client, **kwargs):
        """
        Create a CallCenterSetup and ensure the S3 bucket exists.

        Args:
            s3_client: Boto3 S3 client for creating the bucket
            **kwargs: Additional parameters to pass to the factory

        Returns:
            CallCenterSetup: The created setup with S3 bucket configured
        """
        # Create the call center setup
        setup = cls.create(**kwargs)

        # Set up the S3 bucket
        if s3_client:
            cls._setup_s3_bucket(setup.call_center, s3_client)

        return setup

    @classmethod
    def _setup_s3_bucket(cls, call_center, s3_client):
        """
        Private helper method to set up S3 bucket for a call center.

        Args:
            call_center: The CallCenter instance
            s3_client: Boto3 S3 client
        """
        url_path = S3Path(call_center.s3_storage_path)
        bucket_name = url_path.bucket

        # Check if bucket exists, create if it doesn't
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except Exception:
            # Create bucket with appropriate region configuration
            try:
                s3_client.create_bucket(
                    Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
                )
            except Exception:
                # For us-east-1, don't specify LocationConstraint
                s3_client.create_bucket(Bucket=bucket_name)
