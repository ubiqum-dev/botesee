from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.models.base import Base

if TYPE_CHECKING:
    from src.db.models.elo import Elo
    from src.db.models.player import Player


class Match(Base):
    __tablename__ = "matches"

    id: Mapped[str] = mapped_column(String(length=255), primary_key=True)
    date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.now(tz=UTC), server_default=func.now()
    )
    game: Mapped[str] = mapped_column(
        String(length=64), default="csgo", server_default="csgo"
    )
    stats: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True, default=None, server_default=None
    )

    elos: Mapped[list["Elo"]] = relationship("Elo", back_populates="match")
    players: Mapped[list["Player"]] = relationship(
        "Player", secondary="elos", back_populates="matches", viewonly=True
    )
    # bet_match: Mapped["BetMatch"] = relationship("BetMatch", back_populates="match")

    def __str__(self) -> str:
        return f"<Match {str(self.id)}>"

    def __repr__(self) -> str:
        return f"<Match {str(self.id)}, {self.date}, {self.game}>"
