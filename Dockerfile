FROM python:3.11.8 AS base

RUN pip install -U pip setuptools wheel
RUN pip install pdm

COPY pyproject.toml pdm.lock /botesee/

WORKDIR /botesee
RUN mkdir __pypackages__ && pdm sync --prod --no-editable

ENV PATH "${PATH}:/botesee/__pypackages__/3.11/bin/"
ENV PYTHONPATH "/botesee:/botesee/__pypackages__/3.11/lib:${PYTHONPATH}"
COPY src/ /botesee/src

FROM base AS discord-bot

CMD ["python3", "src/discord_bot/main.py"]

FROM base as web

COPY alembic.ini alembic.ini
COPY web.sh web.sh
RUN chmod +x web.sh
ENTRYPOINT ["./web.sh"]

FROM base as celery

CMD ["celery", "-A", "src.celery.tasks.app", "worker", "-l", "WARNING"]
