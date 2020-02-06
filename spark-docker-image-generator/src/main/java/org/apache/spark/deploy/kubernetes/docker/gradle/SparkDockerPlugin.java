/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.kubernetes.docker.gradle;

import com.palantir.sls.versions.OrderableSlsVersion;
import com.palantir.sls.versions.SlsVersionType;
import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.Transformer;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.Exec;
import org.gradle.api.tasks.Sync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SparkDockerPlugin implements Plugin<Project> {
    private static final Logger log = LoggerFactory.getLogger(SparkDockerPlugin.class);

    public static final String DOCKER_IMAGE_EXTENSION = "sparkDocker";
    public static final String SPARK_JARS_DIR = "jars";
    public static final String SPARK_DOCKER_RUNTIME_CONFIGURATION_NAME = "sparkDockerRuntime";

    @Override
    public void apply(Project project) {
        File dockerBuildDirectory = new File(project.getBuildDir(), "spark-docker-build");
        SparkDockerExtension extension = project.getExtensions().create(
                DOCKER_IMAGE_EXTENSION, SparkDockerExtension.class, project);
        if (!dockerBuildDirectory.isDirectory() && !dockerBuildDirectory.mkdirs()) {
            throw new RuntimeException("Failed to create Docker build directory at "
                    + dockerBuildDirectory.getAbsolutePath());
        }
        Configuration sparkDockerRuntimeConfiguration =
                project.getConfigurations().maybeCreate(SPARK_DOCKER_RUNTIME_CONFIGURATION_NAME);
        File dockerFile = new File(dockerBuildDirectory, "Dockerfile");
        project.getPluginManager().withPlugin("java", plugin -> {
            Configuration runtimeConfiguration = project.getConfigurations().findByName("runtime");
            if (runtimeConfiguration == null) {
                log.warn("No runtime configuration was found for a reference configuration for building"
                        + " your Spark application's docker images.");
            } else {
                sparkDockerRuntimeConfiguration.extendsFrom(runtimeConfiguration);
            }
            Task jarTask = project.getTasks().named("jar").get();
            Provider<File> sparkAppJar = project.getProviders().provider(() ->
                    jarTask.getOutputs().getFiles().getSingleFile());
            Provider<File> jarsDirProvider = project.getProviders().provider(() ->
                    new File(dockerBuildDirectory, SPARK_JARS_DIR));
            Task copySparkAppLibTask = project.getTasks().create(
                    "copySparkAppLibIntoDocker",
                    Copy.class,
                    copyTask -> copyTask.from(
                            project.files(project.getConfigurations()
                                    .getByName(SPARK_DOCKER_RUNTIME_CONFIGURATION_NAME)),
                            sparkAppJar)
                            .into(jarsDirProvider));
            copySparkAppLibTask.dependsOn(jarTask);
            String version = Optional.ofNullable(getClass().getPackage().getImplementationVersion())
                    .orElse("latest.release");
            Configuration dockerResourcesConf = project.getConfigurations().detachedConfiguration(
                    project.getDependencies().create("org.apache.spark:spark-docker-resources:" + version));
            Sync deployScriptsTask = project.getTasks().create(
                    "sparkDockerDeployScripts", Sync.class, task -> {
                        task.from(project.zipTree(dockerResourcesConf.getSingleFile()));
                        task.setIncludeEmptyDirs(false);
                        task.into(dockerBuildDirectory);
                    });
            copySparkAppLibTask.dependsOn(deployScriptsTask);
            GenerateDockerFileTask generateDockerFileTask = project.getTasks().create(
                    "sparkDockerGenerateDockerFile", GenerateDockerFileTask.class);
            generateDockerFileTask.setSrcDockerFile(
                    Paths.get(
                            dockerBuildDirectory.getAbsolutePath(),
                            "kubernetes",
                            "dockerfiles",
                            "spark",
                            "Dockerfile.original").toFile());
            generateDockerFileTask.setDestDockerFile(dockerFile);
            generateDockerFileTask.dependsOn(deployScriptsTask);
            generateDockerFileTask.setBaseImage(extension.getBaseImage());
            Task prepareTask = project.getTasks().create("sparkDockerPrepare");
            prepareTask.dependsOn(
                    deployScriptsTask, generateDockerFileTask, "copySparkAppLibIntoDocker");
        });
        setupDockerTasks(dockerBuildDirectory, dockerFile, project, extension);
    }

    private void setupDockerTasks(
            File buildDirectory,
            File dockerFile,
            Project project,
            SparkDockerExtension extension) {
        Provider<String> resolvedImageName = extension.getImagePath().flatMap(
                (Transformer<Provider<String>, String>) imagePath ->
                        extension.getSnapshotRegistry().flatMap(
                                (Transformer<Provider<String>, String>) snapshotRegistry ->
                                        extension.getReleaseRegistry().map(releaseRegistry ->
                                                resolveImageName(
                                                        project,
                                                        imagePath,
                                                        snapshotRegistry,
                                                        releaseRegistry))));
        DockerBuildTask dockerBuild = project.getTasks().create(
                "sparkDockerBuild",
                DockerBuildTask.class,
                dockerBuildTask -> {
                    dockerBuildTask.setDockerBuildDirectory(buildDirectory);
                    dockerBuildTask.setDockerFile(dockerFile);
                    dockerBuildTask.setImageName(resolvedImageName);
                    dockerBuildTask.dependsOn("sparkDockerPrepare");
                });
        Task tagAllTask = project.getTasks().create("sparkDockerTag");
        LazyExecTask pushAllTask = project.getTasks().create(
                "sparkDockerPush",
                LazyExecTask.class,
                task -> {
                    List<Provider<String>> commandLine = new ArrayList<>();
                    commandLine.add(constProperty(project, "docker"));
                    commandLine.add(constProperty(project, "push"));
                    commandLine.add(resolvedImageName);
                    task.setCommandLine(commandLine);
                });
        project.afterEvaluate(evaluatedProject -> {
            Set<String> tags = extension.getTags().getOrElse(Collections.emptySet());
            List<Exec> tagTasks = tags.stream()
                    .map(tag ->
                            evaluatedProject
                                    .getTasks()
                                    .create(
                                            String.format("sparkDockerTag%s", tag),
                                            Exec.class,
                                            task ->
                                                    task.commandLine(
                                                            "docker",
                                                            "tag",
                                                            resolvedImageName.get(),
                                                            String.format("%s:%s", resolvedImageName.get(), tag))
                                                            .dependsOn(dockerBuild)))
                    .collect(Collectors.toList());
            if (!tagTasks.isEmpty()) {
                tagAllTask.dependsOn(tagTasks);
            } else {
                tagAllTask.dependsOn(dockerBuild);
            }
            List<Exec> pushTasks = tags.stream()
                    .map(tag ->
                            evaluatedProject.getTasks().create(
                                    String.format("sparkDockerPush%s", tag),
                                    Exec.class,
                                    task ->
                                            task.commandLine(
                                                    "docker", "push", String.format("%s:%s", resolvedImageName, tag))
                                                    .dependsOn(String.format("sparkDockerTag%s", tag))))
                    .collect(Collectors.toList());
            if (!pushTasks.isEmpty()) {
                pushAllTask.dependsOn(pushTasks);
            } else {
                pushAllTask.dependsOn(tagAllTask);
            }
        });
    }

    private String resolveImageName(
            Project project,
            String imagePath,
            String snapshotRegistry,
            String releaseRegistry) {
        StringBuilder imageNameBuilder = new StringBuilder();
        if (isReleaseVersion(project.getVersion().toString())) {
            imageNameBuilder.append(releaseRegistry);
        } else {
            imageNameBuilder.append(snapshotRegistry);
        }
        if (!imagePath.startsWith("/")) {
            imageNameBuilder.append('/');
        }
        imageNameBuilder.append(imagePath);
        return imageNameBuilder.toString();
    }

    private Property<String> constProperty(Project project, String value) {
        Property<String> prop = project.getObjects().property(String.class);
        prop.set(value);
        return prop;
    }

    private static boolean isReleaseVersion(String versionString) {
        return OrderableSlsVersion.safeValueOf(versionString)
                .map(version -> version.getType() == SlsVersionType.RELEASE
                        || version.getType() == SlsVersionType.RELEASE_CANDIDATE)
                .orElse(false);
    }
}
