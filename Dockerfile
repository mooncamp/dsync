FROM golang:1.15.0-buster

COPY go.mod /src/
COPY go.sum /src/
RUN cd /src && go mod download
COPY * /src/
RUN cd /src && go build

FROM debian:buster-slim

RUN apt update
RUN apt install -y ca-certificates
RUN update-ca-certificates

COPY --from=0 /src/dsync /usr/local/bin
