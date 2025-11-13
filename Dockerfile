FROM python:3.11-slim-bullseye

ARG GITLAB_USER
ARG GITLAB_PASSWORD

ENV POETRY_HOME=/opt/poetry

ENV PYTHONPATH=/app

RUN apt-get update && \
    apt-get install -y curl git make gcc g++ && \
    apt-get clean && \
    curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s /opt/poetry/bin/poetry /usr/local/bin/poetry && \
    poetry config virtualenvs.create false \

RUN apt-get install apt-utils && apt-get install antiword

WORKDIR /app
COPY . /app

ARG INSTALL_DEV=false

RUN git config --global url."https://$GITLAB_USER:$GITLAB_PASSWORD@gitlab.com/".insteadOf "https://gitlab.com/" && \
    bash -c "if [[ $INSTALL_DEV == 'true' ]]; then poetry install --no-root ; else poetry install --no-root --no-dev ; fi"

RUN chmod +x /app/*.sh