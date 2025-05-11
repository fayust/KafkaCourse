import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.8.10"
    id("checkstyle")
    application
}

group = "ru.job4j"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation("ch.qos.logback:logback-classic:1.4.11")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.7.3")
    implementation("org.apache.kafka:kafka-clients:3.6.0")
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
    jvmArgs("--add-opens", "java.base/java.util.concurrent=ALL-UNNAMED") // Добавьте этот флаг
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "17"
}

application {
    mainClass.set("MainKt")
}

gradle.startParameter.isBuildScan = false