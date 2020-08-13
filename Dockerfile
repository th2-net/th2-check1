FROM gradle:6.4-jdk11 AS build
COPY ./ .
RUN gradle dockerPrepare \
    -Pversion_major=${MAJOR_VERSION} \
    -Pversion_minor=${MINOR_VERSION} \
    -Pversion_maintenance=${MAINTENANCE_VERSION} \
    -Pversion_build=${CI_PIPELINE_IID}

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