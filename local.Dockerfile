FROM python:3.11-slim-bullseye

WORKDIR /app
ARG GITLAB_USER
ARG GITLAB_PASSWORD
ENV POETRY_HOME=/opt/poetry
ENV PATH="$POETRY_HOME/bin:$PATH"
ENV PYTHONPATH=/app

RUN apt-get update && \
    apt-get install -y curl git make gcc g++ bzip2 vim  && \
    apt-get clean && \
    apt-get install -y dos2unix

RUN curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s /opt/poetry/bin/poetry /usr/local/bin/poetry

RUN poetry config virtualenvs.create false

RUN pip install -U pip setuptools wheel

RUN git config --global url."https://$GITLAB_USER:$GITLAB_PASSWORD@gitlab.com/".insteadOf "https://gitlab.com/"

RUN curl -L https://github.com/mit-nlp/MITIE/releases/download/v0.4/MITIE-models-v0.2.tar.bz2 -o /app/MITIE-models-v0.2.tar.bz2

RUN tar -xjf /app/MITIE-models-v0.2.tar.bz2 && \
    rm /app/MITIE-models-v0.2.tar.bz2

RUN mv /app/MITIE-models/english/ner_model.dat /app/ && \
    rm -rf /app/MITIE-models

COPY . /app

RUN chmod +x /app/*.sh

RUN poetry update
