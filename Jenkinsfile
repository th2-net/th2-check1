/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

pipeline {
    agent { label "sailfish" }
    options { timestamps () }
    tools {
        jdk 'openjdk-11.0.2'
    }
    environment {
        VERSION_MAINTENANCE = """${sh(
                            returnStdout: true,
                            script: 'git rev-list --count VERSION-1.1..HEAD'
                            ).trim()}""" //TODO: Calculate revision from a specific tag instead of a root commit
        TH2_SCHEMA_REGISTRY = credentials('TH2_SCHEMA_REGISTRY_USER')
        TH2_SCHEMA_REGISTRY_URL = credentials('TH2_SCHEMA_REGISTRY')
        GRADLE_SWITCHES = " -Pversion_build=${BUILD_NUMBER} -Pversion_maintenance=${VERSION_MAINTENANCE}"
        IDEA_PROPERTIES="idea.ci.properties"
        IDEA_VM_OPTIONS="idea64.ci.vmoptions"
    }
    stages {
        stage('Inspections cleanup') {
            steps {
                sh './gradlew clean'

                dir('inspections') {
                    deleteDir()
                }
            }
        }
        stage('Checkout inspections') {
            steps {
                dir('inspections') {
                    checkout([
                        $class: 'GitSCM',
                        branches: [[name: "*/master"]],
                        doGenerateSubmoduleConfigurations: false,
                        extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: 'intellij-idea-settings']],
                        submoduleCfg: [],
                        userRemoteConfigs: [[credentialsId: 'a0812ef2-e2e5-4b0d-9af8-d7be9c4b86ab', url: 'git@gitlab.exactpro.com:vivarium/th2/intellij-idea-settings.git']]
                    ])
                }
            }
        }
        stage('Run inspections') {
            steps {
                dir('inspections') {
                    // jvm options
                    writeFile file: IDEA_VM_OPTIONS, text: '''\
                        -Xms2048m
                        -Xmx2048m
                        -XX:ReservedCodeCacheSize=240m
                        -XX:+UseConcMarkSweepGC
                        -XX:SoftRefLRUPolicyMSPerMB=50
                        -ea
                        -XX:CICompilerCount=2
                        -Dsun.io.useCanonPrefixCache=false
                        -Djava.net.preferIPv4Stack=true
                        -Djdk.http.auth.tunneling.disabledSchemes=""
                        -XX:+HeapDumpOnOutOfMemoryError
                        -XX:-OmitStackTraceInFastThrow
                        -Djdk.attach.allowAttachSelf=true
                        -Dkotlinx.coroutines.debug=off
                        -Djdk.module.illegalAccess.silent=true
                        -Dawt.useSystemAAFontSettings=lcd
                        -Dsun.java2d.renderer=sun.java2d.marlin.MarlinRenderingEngine
                        -Dsun.tools.attach.tmp.only=true
                    '''.stripIndent()

                    // files filter
                    writeFile file: IDEA_PROPERTIES, text: """\
                            idea.config.path="${pwd()}"
                            idea.log.path="${pwd()}/log"
                            idea.exclude.patterns=\\
                                **/*gradle*;\\
                                **/*.bat;\\
                                **/*.xhtml;\\
                                **/*.png;\\
                                **/*.gif;\\
                                **/*.ftlh;\\
                                **/*.html;\\
                                **/*.jar;\\
                                **/*.json;\\
                                **/*.xml;\\
                                **/*.sql;\\
                                **/*.csv;\\
                                **/*.svg;\\
                                **/*.css;\\
                                **/*.ts;\\
                                **/*.js;\\
                                **/src/gen/**;\\
                                inspections*
                    """.stripIndent()

                    // disable plugins
                    writeFile file: 'disabled_plugins.txt', text: '''\
                            AntSupport
                            ByteCodeViewer
                            Coverage
                            DevKit
                            Git4Idea
                            JUnit
                            Subversion
                            TestNG-J
                            com.android.tools.idea.smali
                            com.intellij.copyright
                            com.intellij.stats.completion
                            com.intellij.tasks
                            com.intellij.uiDesigner
                            com.jetbrains.changeReminder
                            com.jetbrains.sh
                            hg4idea
                            org.editorconfig.editorconfigjetbrains
                            org.intellij.plugins.markdown
                            org.jetbrains.android
                            org.jetbrains.debugger.streams
                            org.jetbrains.idea.eclipse
                            org.jetbrains.plugins.github
                            org.jetbrains.plugins.terminal
                            org.jetbrains.plugins.textmate
                    '''.stripIndent()

                    sh """\
                        ~/idea-IC-193.6494.35/bin/inspect.sh \\
                            "$WORKSPACE" \\
                            "\$(pwd)/intellij-idea-settings/inspection/Evolution.xml" \\
                            "\$(pwd)/results/" \\
                            -v2
                    """.stripIndent()
                }
            }
        }
        stage('Publish inspections report') {
            steps {
                dir('inspections') {
                    recordIssues enabledForFailure: true,
                        tool: ideaInspection(pattern: '**/results/*.xml'),
                        qualityGates: [[threshold: 1, type: 'TOTAL_HIGH', failed: true]]
                }
            }
        }
        stage ('Artifactory configuration') {
            steps {
                rtGradleDeployer (
                    id: "GRADLE_DEPLOYER",
                    serverId: "artifatory5",
                    repo: "th2-schema-snapshot-local",
                )

                rtGradleResolver (
                    id: "GRADLE_RESOLVER",
                    serverId: "artifatory5",
                    repo: "th2-schema-snapshot-local"
                )
            }
        }
        stage ('Config Build Info') {
            steps {
                rtBuildInfo (
                    captureEnv: true
                )
            }
        }

        stage('Build') {
            steps {
                rtGradleRun (
                    usesPlugin: true, // Artifactory plugin already defined in build script
                    useWrapper: true,
                    rootDir: "./",
                    buildFile: 'build.gradle',
                    tasks: "clean build artifactoryPublish ${GRADLE_SWITCHES}",
                    deployerId: "GRADLE_DEPLOYER",
                    resolverId: "GRADLE_RESOLVER",
                )
            }
        }
        stage ('Publish build info') {
            steps {
                rtPublishBuildInfo (
                    serverId: "artifatory5"
                )
            }
        }
        stage('Publish docker') {
            steps {
                sh """
                    docker login -u ${TH2_SCHEMA_REGISTRY_USR} -p ${TH2_SCHEMA_REGISTRY_PSW} ${TH2_SCHEMA_REGISTRY_URL}
                    ./gradlew dockerPush ${GRADLE_SWITCHES} \
                    -Ptarget_docker_repository=${TH2_SCHEMA_REGISTRY_URL}
                """
            }
        }
        stage('Publish report') {
            steps {
                script {
                    def properties = readProperties  file: 'gradle.properties'
                    def dockerImageVersion = "${properties['version_major'].trim()}.${properties['version_minor'].trim()}.${VERSION_MAINTENANCE}.${BUILD_NUMBER}"

                    def changeLogs = ""
                    try {
                        def changeLogSets = currentBuild.changeSets
                        for (int changeLogIndex = 0; changeLogIndex < changeLogSets.size(); changeLogIndex++) {
                            def entries = changeLogSets[changeLogIndex].items
                            for (int itemIndex = 0; itemIndex < entries.length; itemIndex++) {
                                def entry = entries[itemIndex]
                                changeLogs += "\n${entry.msg}"
                            }
                        }
                    } catch(e) {
                        println "Exception occurred: ${e}"
                    }

                    currentBuild.description = "docker-image-version = ${dockerImageVersion}"
                }
            }
        }
    }
}
