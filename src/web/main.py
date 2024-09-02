from fastapi import (
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Request,
    status,
    BackgroundTasks,
)
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger
from pydantic import BaseModel

from src import conf
from src.celery.tasks import match_score_update, match_finished
from src.clients.models.rabbit.queues import QueueName
from src.clients.rabbit import RabbitClient
from src.clients.redis_repo import redis_repo
from src.web.dependencies import get_rabbit
from src.web.models.base import EventEnum
from src.web.models.events import WebhookMatch

logger.add("errors.log", level="ERROR", rotation="1 week")
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    exc_str = f"{exc}".replace("\n", " ").replace("   ", " ")
    logger.error(f"{request}: {exc_str}")
    logger.error(f"entity = {exc.body}")
    content = {
        "status_code": status.HTTP_422_UNPROCESSABLE_ENTITY,
        "message": exc_str,
        "data": exc.body,
    }
    return JSONResponse(
        content=content, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
    )


async def faceit_webhook_auth(
    token: str = Header(None, alias="Webhook-Authorization"),
) -> str:
    if token != conf.FACEIT_WEBHOOK_AUTH:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Authorization required")
    return token


class OKResponse(BaseModel):
    status: str = "ok"


@app.get("/", tags=["health"])
@app.head("/", tags=["health"])
@app.get("/health", tags=["health"])
async def health() -> OKResponse:
    return OKResponse()


@app.post("/webhooks/faceit", tags=["Webhooks"])
async def faceit_webhook(
    match: WebhookMatch,
    background_tasks: BackgroundTasks,
    rabbit: RabbitClient = Depends(get_rabbit),
    token: str = Depends(faceit_webhook_auth),
) -> OKResponse:
    logger.info(f"{match.json()}")

    match match.event:
        case EventEnum.CONFIGURING:
            match_score_update.delay(match.dict())
            background_tasks.add_task(
                rabbit.publish, message=match.json(), routing_key=QueueName.MATCHES
            )
            background_tasks.add_task(
                redis_repo.publish,
                queue=QueueName.MATCHES,
                message=match,
            )
        case EventEnum.FINISHED:
            match_finished.delay(match.dict())
        case _:
            background_tasks.add_task(
                rabbit.publish, message=match.json(), routing_key=QueueName.MATCHES
            )
            background_tasks.add_task(
                redis_repo.publish,
                queue=QueueName.MATCHES,
                message=match,
            )

    return OKResponse()
