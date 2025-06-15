import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.8.10"
    id("org.springframework.boot") version "3.0.0"
    id("io.spring.dependency-management") version "1.0.15.RELEASE"
    id("checkstyle")
    application
}

group = "ru.job4j"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

application {
    mainClass.set("ru.job4j.kafka.KafkaCourseApplication.kt")
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation("ch.qos.logback:logback-classic:1.4.7")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.7.3")
    implementation("org.apache.kafka:kafka-clients:3.6.0")

    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-logging")
    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    testImplementation(kotlin("test"))
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.0")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.14.0")
    testImplementation ("org.junit.jupiter:junit-jupiter:5.8.1")
    testImplementation("org.junit.platform:junit-platform-launcher:1.8.0")
    testImplementation("io.mockk:mockk:1.13.17")
    testImplementation("org.hamcrest:hamcrest:3.0")
}

kotlin {
    jvmToolchain {
        (this).languageVersion.set(JavaLanguageVersion.of(17)) // версия Java 17
    }
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "17"
    kotlinOptions {
        jvmTarget = "17" // версия JVM для Kotlin
    }
}

checkstyle {
    toolVersion = "8.45"
    configFile = file("${project.rootDir}/config/checkstyle/checkstyle.xml")
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--add-opens", "java.base/java.util.concurrent=ALL-UNNAMED")
}

gradle.startParameter.isBuildScan = false