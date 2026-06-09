plugins {
    alias(libs.plugins.kotlin.jvm)
    kotlin("plugin.serialization") version "2.4.0"
}

group = "io.kcelery"
version = "0.0.1"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation(libs.kotlinx.serialization)
    implementation(libs.lattuce.core)
    implementation(libs.cron.utils)
    implementation(libs.kotlinx.coroutines)
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactive")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("ch.qos.logback:logback-classic:1.4.11")

    // Metrics
    implementation("io.micrometer:micrometer-core:1.11.5")
    implementation("io.micrometer:micrometer-registry-prometheus:1.11.5")

    // Distributed locking
    implementation("org.redisson:redisson:3.24.3")

    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}