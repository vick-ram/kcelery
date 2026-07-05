plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}
rootProject.name = "kcelery"

include("kcelery-core")
include("kcelery-redis")
include("kcelery-metrics")
include("kcelery-examples")