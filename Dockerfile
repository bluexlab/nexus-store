FROM golang:1.23.1-alpine3.19 AS builder

ARG BUILDNO=local

ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    GOPROXY=https://proxy.golang.org,direct

RUN apk add --no-cache \
    binutils \
    ca-certificates \
    tzdata

# setup the working directory
WORKDIR /app/src

# Download dependencies
COPY go.mod /app/src/
COPY go.sum /app/src/
RUN go mod download

# add source code
COPY . /app/src/

# build the nexus-store
RUN cd /app/src \
    && echo $BUILDNO > BUILD \
    && go build -o /go/bin/nexus-store ./app/server

# FROM scratch
FROM alpine:3.19

ARG BUILDNO=local
ARG REV=unknown
ARG APP_HOME=/app
ENV PATH=$APP_HOME:$PATH

WORKDIR $APP_HOME

COPY --from=builder /usr/local/go/lib/time/zoneinfo.zip /usr/local/go/lib/time/zoneinfo.zip
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /go/bin/nexus-store $APP_HOME/nexus-store
COPY migrate.sh $APP_HOME/migrate.sh

# add launch shell command
COPY docker-entrypoint.sh /usr/bin/

RUN echo $BUILDNO > $APP_HOME/BUILD \
    && echo $REV > $APP_HOME/REV

RUN addgroup -S appgroup && adduser -h $APP_HOME -G appgroup -S -D -H appuser
RUN chown -R appuser:appgroup $APP_HOME

USER appuser

ENV PORT=5000

EXPOSE 5000

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["./nexus-store"]