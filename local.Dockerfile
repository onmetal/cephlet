FROM debian:trixie AS builder

RUN apt update  && apt install -y golang git libcephfs-dev librbd-dev librados-dev libc-bin

WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum

COPY .netrc /root/
RUN go mod download
