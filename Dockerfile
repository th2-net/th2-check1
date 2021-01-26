FROM gradle:6.6-jdk11 AS build
ARG release_version
ARG artifactory_user
ARG artifactory_password
ARG artifactory_deploy_repo_key
ARG artifactory_url

COPY ./ .
RUN gradle --stacktrace clean build dockerPrepare artifactoryPublish \
     -Prelease_version=${release_version} \
     -Partifactory_user=${artifactory_user} \
     -Partifactory_password=${artifactory_password} \
     -Partifactory_deploy_repo_key=${artifactory_deploy_repo_key} \
     -Partifactory_url=${artifactory_url}

FROM adoptopenjdk/openjdk11:alpine
ENV RABBITMQ_HOST=rabbitmq \
    RABBITMQ_PORT=5672 \
    RABBITMQ_USER=guest \
    RABBITMQ_PASS=guest \
    RABBITMQ_VHOST=th2 \
    GRPC_PORT=8080
WORKDIR /home
COPY --from=build /home/gradle/build/docker ./
ENTRYPOINT ["/home/service/bin/service"]