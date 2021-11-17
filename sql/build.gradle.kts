import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.6.0"
    id("org.beryx.runtime") version "1.12.7"
}

application {
    mainClass.set("app.logflare.sql.Main")
}


group = "app.logflare"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(platform("com.google.cloud:libraries-bom:20.7.0"))
    implementation("com.google.cloud:google-cloud-bigquery")
    implementation(fileTree("lib") { include("*.jar") })

    implementation(fileTree("gsp") { include("*.jar") })
    implementation("tk.pratanumandal:expr4j:0.0.3")
    implementation("org.junit.jupiter:junit-jupiter:5.7.0")
    implementation("com.alibaba:fastjson:1.2.76")
    implementation("org.asm-labs:junrar:0.8")
    implementation("org.simpleframework:simple-xml:2.7.1")
    implementation("jdom:jdom:1.1")
    implementation("net.robyf:gradle-jarinjar-plugin:1.0.9")

    implementation(group = "com.impossibl.pgjdbc-ng", name = "pgjdbc-ng", version = "0.8.9")
    implementation(group = "com.zaxxer", name = "HikariCP", version = "4.0.3")
    implementation("ch.qos.logback:logback-classic:1.2.3")
    implementation("ch.qos.logback:logback-core:1.2.3")
    implementation("commons-logging:commons-logging:1.2")

    testImplementation(kotlin("test"))
    testImplementation(kotlin("test-junit"))
    testImplementation("io.mockk:mockk:1.12.0")
}

tasks.test {
    useJUnit()
}

tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "13"
}

runtime {
    options.set(listOf("--strip-debug", "--compress", "2", "--no-header-files", "--no-man-pages"))
    modules.set(listOf("java.naming","java.desktop", "jdk.unsupported", "java.scripting", "java.logging",
        "java.xml", "java.sql"))
    this.imageDir.set(File("$projectDir/../priv/sql"))
}
