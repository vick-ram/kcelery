plugins {
    alias(libs.plugins.kotlin.jvm)
    kotlin("plugin.serialization") version "2.4.0"
    id("maven-publish") // Add the maven-publish plugin
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
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.coroutines.reactive)

    // Logging
    implementation(libs.slf4j.logging)
    implementation(libs.logback.classic)

    // Metrics
    implementation(libs.micrometer.core)
    implementation(libs.micrometer.registry.prometheus)

    // Distributed locking
    implementation(libs.redisson) { exclude(group = "io.netty") }

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
    testImplementation("io.mockk:mockk:1.13.12")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.3")
    testImplementation("org.mockito:mockito-junit-jupiter:5.12.0") // Added Mockito JUnit Jupiter dependency
    testImplementation("org.testcontainers:testcontainers:2.0.5")
    testImplementation("org.testcontainers:junit-jupiter:1.21.4")
    testImplementation("org.junit.platform:junit-platform-suite:1.11.0")
}

kotlin {
    jvmToolchain(21)
//    compilerOptions {
//        freeCompilerArgs.addAll(
//            "-Xjsr305=strict",
//            "-Xcontext-receivers"
//        )
//    }
}

java {
    withSourcesJar()
    withJavadocJar()
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true
    }
}

tasks.withType<Jar> {
    manifest {
        attributes["Implementation-Title"] = "KCelery"
        attributes["Implementation-Version"] = archiveVersion
        attributes["Implementation-Vendor"] = "KCelery Contributors"
    }
}

// Configure publishing for JitPack
publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = project.group.toString()
            artifactId = project.name // This will use the project name, e.g., "kcelery"
            version = project.version.toString()

            from(components["kotlin"])

            pom {
                name.set("KCelery")
                description.set("Distributed task queue and scheduler for Kotlin, inspired by Celery")
                url.set("https://github.com/vick-ram/kcelery")

                licenses {
                    license {
                        name.set("MIT License")
                        url.set("https://opensource.org/licenses/MIT")
                    }
                }

                developers {
                    developer {
                        id.set("vickram")
                        name.set("Vickram Odero")
                        email.set("vickramodero6@gmail.com")
                    }
                }
            }
        }
    }

    repositories {
        maven {
            name ="jitpack"
            url =uri("https://jitpack.io")
        }
    }
}