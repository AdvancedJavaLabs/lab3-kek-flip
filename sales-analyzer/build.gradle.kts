plugins {
    id("java")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

group = "org.itmo"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.apache.hadoop:hadoop-common:3.4.2")
    compileOnly("org.apache.hadoop:hadoop-mapreduce-client-core:3.4.2")
}
