from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel


class EventEnum(str, Enum):
    ABORTED = "match_status_aborted"
    CANCELLED = "match_status_cancelled"
    FINISHED = "match_status_finished"
    READY = "match_status_ready"
    CONFIGURING = "match_status_configuring"


class Entity(BaseModel):
    id: UUID
    name: str
    type: str


class Player(BaseModel):
    id: UUID
    nickname: str
    avatar: str
    game_id: str
    game_name: str | None
    game_skill_level: int
    membership: str | None
    anticheat_required: bool


class Team(BaseModel):
    id: UUID
    name: str
    type: UUID | str | None
    avatar: str
    leader_id: UUID
    co_leader_id: UUID | str | None
    roster: list[Player]
    substitutions: int
    substitutes: list[Player] | None


class BasePayload(BaseModel):
    id: str
    organizer_id: str
    region: str
    game: str
    version: int
    entity: Entity
    teams: list[Team]


class BaseMatch(BaseModel):
    transaction_id: UUID
    event: EventEnum
    event_id: UUID
    third_party_id: UUID
    app_id: UUID
    timestamp: datetime
    retry_count: int
    version: int

    class Config:
        use_enum_values = True
