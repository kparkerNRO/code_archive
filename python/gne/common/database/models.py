import datetime
import json

from common.database.encryption import get_encryption_key
from common.models.db_enum import ProcessStatus, RejectedPreReason, ReportedStatus
from pydantic import BaseModel, field_serializer
from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy_utils.types.encrypted.encrypted_type import AesEngine, StringEncryptedType
from sqlalchemy_utils.types.json import JSONType


class PydanticJSONType(JSONType):
    """A custom SQLAlchemy type for automatic Pydantic model serialization/deserialization."""

    impl = JSON
    cache_ok = True

    def __init__(self, pydantic_model):
        self.pydantic_model = pydantic_model
        super().__init__()

    def process_bind_param(self, value, dialect):
        """Convert Pydantic model to JSON string for storage."""
        if value is None:
            return None
        if hasattr(value, "model_dump_json"):  # Check if it's a Pydantic model
            return value.model_dump_json(exclude_none=True)
        elif isinstance(value, dict):
            # If it's already a dict, validate and convert it to the model first
            model_instance = self.pydantic_model.model_validate(value)
            return model_instance.model_dump_json(exclude_none=True)
        return super().process_bind_param(value, dialect)

    def process_result_value(self, value, dialect):
        """Convert JSON string from database to Pydantic model."""
        if value is None:
            return None
        value = json.loads(value)
        return self.pydantic_model.model_validate(value)


class EncryptableJSONType(JSONType):
    impl = JSONType

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return json.dumps(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return json.loads(value)


class Base(DeclarativeBase):
    pass


class CallCenter(Base):
    __tablename__ = "call_center"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    external_id: Mapped[str] = mapped_column(String(255), nullable=False)  # not sure if this is UUID or INT
    name: Mapped[str] = mapped_column(Text, nullable=False)
    s3_storage_path: Mapped[str] = mapped_column(Text, nullable=True)
    created_ts: Mapped[datetime.datetime] = mapped_column(DateTime, default=func.now(timezone="UTC"))
    updated_ts: Mapped[datetime.datetime] = mapped_column(
        DateTime, default=func.now(timezone="UTC"), onupdate=func.now(timezone="UTC")
    )

    def __repr__(self) -> str:
        return f"<CallCenter>(id: {self.id}, name: {self.name})"


class CallCenterUser(Base):
    __tablename__ = "call_center_user"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    email: Mapped[str] = mapped_column(Text, nullable=False)

    call_center_id: Mapped[int] = mapped_column(Integer, ForeignKey("call_center.id"), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_ts: Mapped[datetime.datetime] = mapped_column(DateTime, default=func.now(timezone="UTC"))
    updated_ts: Mapped[datetime.datetime] = mapped_column(
        DateTime, default=func.now(timezone="UTC"), onupdate=func.now(timezone="UTC")
    )

    # Relationships
    call_center: Mapped["CallCenter"] = relationship("CallCenter")
    recordings: Mapped[list["Recording"]] = relationship("Recording", back_populates="call_center_user")

    def __repr__(self) -> str:
        return f"<CallCenterUser>(id: {self.id}, display_name: {self.display_name})"


class UserActivity(Base):
    __tablename__ = "user_activity"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Mapping activity via email because not all users (supervisors) will have a talkdesk user
    email: Mapped[str] = mapped_column(Text, nullable=False)

    activity: Mapped[str] = mapped_column(Text, nullable=False)
    activity_ts: Mapped[str] = mapped_column(DateTime, default=func.now())

    # metadata is reserved by sqlalchemy
    activity_metadata: Mapped[dict] = mapped_column(
        MutableDict.as_mutable(JSON), nullable=True, server_default=text("'{}'")
    )

    created_ts: Mapped[datetime.datetime] = mapped_column(DateTime, default=func.now(timezone="UTC"))
    updated_ts: Mapped[datetime.datetime] = mapped_column(
        DateTime, default=func.now(timezone="UTC"), onupdate=func.now(timezone="UTC")
    )


class Recording(Base):
    __tablename__ = "recording"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    call_center_user_id: Mapped[int] = mapped_column(Integer, ForeignKey("call_center_user.id"), nullable=False)
    call_id: Mapped[int] = mapped_column(Integer, ForeignKey("call.id"), nullable=False)
    external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    audio_url: Mapped[str] = mapped_column(String(255), nullable=True)
    transcript_url: Mapped[str] = mapped_column(String(255), nullable=True)
    start_time: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=True)
    duration: Mapped[float] = mapped_column(Float, nullable=True)

    patients: Mapped[list["Patient"]] = relationship("Patient", back_populates="recording")

    created_ts: Mapped[datetime.datetime] = mapped_column(DateTime, default=func.now(timezone="UTC"))
    updated_ts: Mapped[datetime.datetime] = mapped_column(
        DateTime, default=func.now(timezone="UTC"), onupdate=func.now(timezone="UTC")
    )

    # Relationships
    call_center_user: Mapped["CallCenterUser"] = relationship("CallCenterUser", back_populates="recordings")
    call: Mapped["Call"] = relationship("Call", back_populates="recordings")
    call_processes: Mapped[list["CallProcess"]] = relationship("CallProcess", back_populates="recording")

    def __repr__(self) -> str:
        return f"<Recording>(id: {self.id}, external_id: {self.external_id})"


class Call(Base):
    __tablename__ = "call"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    external_id: Mapped[str] = mapped_column(String(255), nullable=True)
    call_center_id: Mapped[int] = mapped_column(Integer, ForeignKey("call_center.id"), nullable=False)

    start_time: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=True)
    end_time: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=True)

    created_ts: Mapped[datetime.datetime] = mapped_column(DateTime, default=func.now(timezone="UTC"))
    updated_ts: Mapped[datetime.datetime] = mapped_column(
        DateTime, default=func.now(timezone="UTC"), onupdate=func.now(timezone="UTC")
    )

    # Relationships
    call_center: Mapped["CallCenter"] = relationship("CallCenter")
    recordings: Mapped[list["Recording"]] = relationship("Recording", back_populates="call")
    call_processes: Mapped[list["CallProcess"]] = relationship("CallProcess", back_populates="call")

    def __repr__(self) -> str:
        return f"<Call>(id: {self.id}, external_id: {self.external_id})"


class Patient(Base):
    __tablename__ = "patient"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    recording_id: Mapped[int] = mapped_column(Integer, ForeignKey("recording.id"), nullable=False)
    patient_name: Mapped[str] = mapped_column(
        StringEncryptedType(String(255), get_encryption_key, AesEngine, "pkcs5"), nullable=True
    )
    date_of_birth: Mapped[datetime.datetime] = mapped_column(
        StringEncryptedType(DateTime, get_encryption_key, AesEngine, "pkcs5"), nullable=True
    )
    gender: Mapped[str] = mapped_column(String(50), nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)

    products: Mapped[list["Product"]] = relationship("Product")
    recording: Mapped["Recording"] = relationship("Recording", back_populates="patients")

    def __repr__(self) -> str:
        return f"<Patient>(id: {self.id}, name: {self.patient_name})"


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    recording_id: Mapped[int] = mapped_column(Integer, ForeignKey("recording.id"), nullable=False)
    patient_id: Mapped[int] = mapped_column(Integer, ForeignKey("patient.id"), nullable=True)

    patient: Mapped["Patient"] = relationship("Patient", back_populates="products")

    def __repr__(self) -> str:
        return f"<Product>(id: {self.id}, name: {self.name})"


class CallProcessMessage(BaseModel):
    """
    Standardized messaging format for the status text
    """

    message: str
    exception: Exception | str | None = None

    @field_serializer("exception")
    def serialize_exception(self, exception: Exception, _info):
        return repr(exception)

    class Config:
        json_encoders = {Exception: lambda v: repr(v)}
        exclude_none = True
        exclude_unset = True
        arbitrary_types_allowed = True


class CallProcess(Base):
    __tablename__ = "call_process"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    call_id: Mapped[int] = mapped_column(Integer, ForeignKey("call.id"), nullable=False)
    recording_id: Mapped[int] = mapped_column(Integer, ForeignKey("recording.id"), nullable=True)
    status_source: Mapped[str] = mapped_column(String(255), nullable=False)
    process_status: Mapped[ProcessStatus] = mapped_column(String(255), nullable=False)
    created_ts: Mapped[datetime.datetime] = mapped_column(DateTime, default=func.now(timezone="UTC"))
    status_text: Mapped[CallProcessMessage] = mapped_column(
        PydanticJSONType(CallProcessMessage), nullable=True, comment="Additional status information"
    )

    # Relationships
    call: Mapped["Call"] = relationship("Call", back_populates="call_processes")
    recording: Mapped["Recording | None"] = relationship("Recording", back_populates="call_processes")

    def __repr__(self) -> str:
        return f"<CallProcess>(id: {self.id}, status: {self.process_status})"


class PotentialReportableEvent(Base):
    __tablename__ = "potential_reportable_event"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    recording_id: Mapped[int] = mapped_column(Integer, ForeignKey("recording.id"), nullable=False)
    reported_status: Mapped[ReportedStatus | None] = mapped_column(String(100), nullable=True)
    external_aero_id: Mapped[str] = mapped_column(String(255), nullable=True)

    # AE detection fields - Event details
    confidence_score: Mapped[float] = mapped_column(Float, nullable=False)
    evidence: Mapped[list[str]] = mapped_column(
        StringEncryptedType(EncryptableJSONType, get_encryption_key, AesEngine, "pkcs5"), nullable=False
    )  # Corresponds to event_details AE service response

    # AE detection fields - Reporter details
    reporter_name: Mapped[str] = mapped_column(
        StringEncryptedType(Text, get_encryption_key, AesEngine, "pkcs5"), nullable=True
    )
    reporter_type: Mapped[str] = mapped_column(Text, nullable=True)
    reporter_contact: Mapped[str] = mapped_column(
        StringEncryptedType(Text, get_encryption_key, AesEngine, "pkcs5"), nullable=True
    )
    permission_to_follow_up: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # AE confirmation fields
    confirmed_ae: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    rejected_ae_reason: Mapped[RejectedPreReason] = mapped_column(Text, nullable=True)
    other_rejected_reason: Mapped[str] = mapped_column(Text, nullable=True)
    adjudicated_by: Mapped[str] = mapped_column(Text, nullable=True)

    ae_response: Mapped[dict] = mapped_column(
        StringEncryptedType(EncryptableJSONType, get_encryption_key, AesEngine, "pkcs5"), nullable=True
    )

    created_ts: Mapped[datetime.datetime] = mapped_column(DateTime, default=func.now(timezone="UTC"))
    updated_ts: Mapped[datetime.datetime] = mapped_column(
        DateTime, default=func.now(timezone="UTC"), onupdate=func.now(timezone="UTC")
    )

    recording: Mapped["Recording"] = relationship("Recording")

    def __repr__(self) -> str:
        return (
            f"<PotentialReportableEvent>(id: {self.id}, "
            f"reported_status: {self.reported_status}, confidence: {self.confidence_score})"
        )


class DeepgramApiCall(Base):
    __tablename__ = "deepgram_api_call"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    recording_id: Mapped[int] = mapped_column(Integer, ForeignKey("recording.id"), nullable=False)
    recording_duration: Mapped[float] = mapped_column(Float, nullable=True)
    length: Mapped[int] = mapped_column(Integer, nullable=True)
    elapsed_time: Mapped[float] = mapped_column(Float, nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    api_response: Mapped[dict] = mapped_column(JSON, nullable=True)
    success: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    error_message: Mapped[str] = mapped_column(Text, nullable=True)
    created_ts: Mapped[datetime.datetime] = mapped_column(DateTime, default=func.now(timezone="UTC"))
    updated_ts: Mapped[datetime.datetime] = mapped_column(
        DateTime, default=func.now(timezone="UTC"), onupdate=func.now(timezone="UTC")
    )

    def __repr__(self) -> str:
        return f"<DeepgramApiCall>(id: {self.id}, recording_id: {self.recording_id})"


class CallCenterSsoGroupMap(Base):
    __tablename__ = "call_center_sso_group_map"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    call_center_id: Mapped[int] = mapped_column(Integer, ForeignKey("call_center.id"), nullable=False)
    sso_group_name: Mapped[str] = mapped_column(Text, nullable=True)
    is_supervisor: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_ts: Mapped[datetime.datetime] = mapped_column(DateTime, default=func.now(timezone="UTC"))
    updated_ts: Mapped[datetime.datetime] = mapped_column(
        DateTime, default=func.now(timezone="UTC"), onupdate=func.now(timezone="UTC")
    )

    call_center: Mapped["CallCenter"] = relationship("CallCenter")
