FROM gradle:6.4-jdk11 AS build
COPY ./ .
RUN gradle clean build dockerPrepare -Pversion_build=${CI_PIPELINE_IID} \
    -Partifactory_user=${ARTIFACTORY_USER} \
    -Pversion_major=${MAJOR_VERSION} -Pversion_minor=${MINOR_VERSION} \
    -Partifactory_password=${ARTIFACTORY_PASS} \
    -Partifactory_deploy_repo_key=${ARTIFACTORY_REPO} \
    -Partifactory_url=${ARTIFACTORY_URL} \
    -Partifactory_resolve_repo=${ARTIFACTORY_RESOLVE_REPO} \
    -Pdocker_image_name=${DOCKER_IMAGE_NAME}
FROM openjdk:12-alpine
ENV RABBITMQ_HOST=rabbitmq \
    RABBITMQ_PORT=5672 \
    RABBITMQ_USER=guest \
    RABBITMQ_PASS=guest \
    RABBITMQ_VHOST=th2 \
    GRPC_PORT=8080
WORKDIR /home
COPY --from=build /home/gradle/build/docker ./
ENTRYPOINT ["/home/verifier-service/bin/verifier-service", "/home/verifier-service/etc/config.yml"]