plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    application
}

group = "io.github.origincoding"
version = "0.1.0-SNAPSHOT"

kotlin {
    jvmToolchain(25)
}

application {
    mainClass.set("io.github.origincoding.gossipglomers.MainKt")
}

dependencies {
    implementation(libs.kotlinx.serialization.json)
}

tasks.withType<JavaExec>().configureEach {
    standardInput = System.`in`
}

tasks.wrapper {
    gradleVersion = "9.8.2"
    distributionType = Wrapper.DistributionType.BIN
    distributionUrl = "https://mirrors.cloud.tencent.com/gradle/gradle-9.8.2-bin.zip"
    distributionSha256Sum = "21bd5da2de49b642904077f38c0efdc7778e92b3ed0889392dca61b97e389607"
    networkTimeout = 60000
}
