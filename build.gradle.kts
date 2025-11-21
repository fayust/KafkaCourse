import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    val kotlinVersion = "2.0.0"
    kotlin("jvm") version kotlinVersion
    id("org.springframework.boot") version "3.3.5"
    id("io.spring.dependency-management") version "1.1.6"
    kotlin("plugin.spring") version kotlinVersion
    id("checkstyle")
    id("application")
}

group = "ru.job4j.kafka"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

application {
    mainClass.set("ru.job4j.kafka.KafkaCourseApplicationKt")
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
    implementation("org.apache.kafka:kafka-clients:3.8.0")
    implementation("org.springframework.kafka:spring-kafka:3.3.5")
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-logging")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.0")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.14.0")

    implementation("org.springframework.boot:spring-boot-starter-data-mongodb")
    implementation("io.mongock:mongock-springboot-v3:5.5.1")
    implementation("io.mongock:mongodb-sync-v4-driver:5.5.1")

    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.8.1")
    testImplementation("org.junit.platform:junit-platform-launcher:1.8.0")
    testImplementation("io.mockk:mockk:1.13.17")
    testImplementation("org.hamcrest:hamcrest:3.0")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("de.flapdoodle.embed:de.flapdoodle.embed.mongo:4.12.2")

    testImplementation("org.testcontainers:junit-jupiter:1.20.2")
    testImplementation("org.testcontainers:kafka:1.20.2")
    testImplementation("org.apache.kafka:kafka-clients:3.6.0")
    testImplementation("org.jetbrains.kotlin:kotlin-test:2.0.0")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.1")

}

kotlin {
    jvmToolchain {
        (this).languageVersion.set(JavaLanguageVersion.of(17))
    }
}

tasks.withType<KotlinCompile> {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

checkstyle {
    toolVersion = "8.45"
    configFile = file("${project.rootDir}/config/checkstyle/checkstyle.xml")
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--add-opens", "java.base/java.util.concurrent=ALL-UNNAMED")
    filter {
        excludeTestsMatching("ru.job4j.kafka.reqreply.integration.*")
    }
}

//gradle runIntegrationTests --info
//gradle runIntegrationTests --debug > debug.log
tasks.register<Test>("runIntegrationTests") {
    useJUnitPlatform()
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath

    include("ru/job4j/kafka/reqreply/integration/*")

    doLast {
        println("Integration tests have been executed.")
    }
}

gradle.startParameter.isBuildScan = false