FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /

COPY bin/update /update
COPY docs /docs
COPY internal/infrastructure/mariadb/migrations /internal/infrastructure/mariadb/migrations

EXPOSE 8080

ENTRYPOINT ["/update"]
