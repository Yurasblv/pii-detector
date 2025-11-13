FROM registry.gitlab.com/ariksa/docker-images/poetry:latest AS builder
FROM registry.gitlab.com/ariksa/docker-images/python:latest

COPY --from=builder /usr/local/lib/ /usr/local/lib
COPY --from=builder /usr/local/bin/ /usr/local/bin

# Install Antiword
RUN apt-get update && apt-get install apt-utils && apt-get install antiword
