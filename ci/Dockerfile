FROM gradle:6.4-jdk11 AS build
COPY ./ .
RUN gradle --no-daemon clean
FROM slon19/ideainspection AS inspect
COPY --from=build /home/gradle ./
COPY ./ci/idea ./
ARG CI_JOB_TOKEN
ENV IDEA_PROPERIES=/opt/idea/idea.ci.properties
ENV CI_JOB_TOKEN=$CI_JOB_TOKEN
RUN git clone https://gitlab-ci-token:${CI_JOB_TOKEN}@gitlab.exactpro.com/vivarium/th2/intellij-idea-settings.git
RUN /opt/idea/bin/inspect.sh ./ ./intellij-idea-settings/inspection/Evolution.xml ./results -v2 -format plain
FROM build as compile
RUN gradle build -Pversion_build=${CI_PIPELINE_IID} \
    -Pversion_maintenance=${VERSION_MAINTENANCE} \
    -Partifactory_user=${ARTIFACTORY_USER} \
    -Partifactory_password=${ARTIFACTORY_PASS} \
    -Partifactory_deploy_repo_key=${ARTIFACTORY_REPO} \
    -Partifactory_url=${ARTIFACTORY_URL} \
    -Partifactory_resolve_repo=${ARTIFACTORY_RESOLVE_REPO}
RUN gradle -q getImageVersion -Pversion_build=${CI_PIPELINE_IID} \
           -Pversion_maintenance=${VERSION_MAINTENANCE} > version.txt && \
    gradle -q getImageName -Pversion_build=${CI_PIPELINE_IID} \
           -Pversion_maintenance=${VERSION_MAINTENANCE} > imageName.txt
FROM openjdk:12-alpine
ENV RABBITMQ_HOST=rabbitmq \
    RABBITMQ_PORT=5672 \
    RABBITMQ_USER=guest \
    RABBITMQ_PASS=guest \
    RABBITMQ_VHOST=th2 \
    GRPC_PORT=8080
WORKDIR /home
COPY --from=inspect /opt/idea ./inspections
COPY --from=compile /home/gradle ./
ENTRYPOINT ["/home/verifier-service/bin/verifier-service", "/home/verifier-service/etc/config.yml"]