FROM gradle:6.6-jdk11 AS build
ARG app_version=0.0.0
COPY ./ .
RUN gradle dockerPrepare -Prelease_version=${app_version}

FROM openjdk:12-alpine
ENV RABBITMQ_HOST=rabbitmq \
    RABBITMQ_PORT=5672 \
    RABBITMQ_USER=guest \
    RABBITMQ_PASS=guest \
    RABBITMQ_VHOST=th2 \
    GRPC_PORT=8080
WORKDIR /home
COPY --from=build /home/gradle/build/docker ./
ENTRYPOINT ["/home/service/bin/service", "/home/service/etc/config.yml"]