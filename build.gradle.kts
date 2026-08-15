plugins {
    scala
    `maven-publish`
}

val scalaVersion = providers.gradleProperty("scalaVersion").get()
val scalaBinaryVersion = providers.gradleProperty("scalaBinaryVersion").get()
val sparkVersion = providers.gradleProperty("sparkVersion").get()
val scalatestVersion = providers.gradleProperty("scalatestVersion").get()
val deltaVersion = providers.gradleProperty("deltaVersion").get()
val jacksonVersion = providers.gradleProperty("jacksonVersion").get()
val artifactName = providers.gradleProperty("artifactName").get()

java {
    // The Scala compiler targets Java 17 below; JDK 21 is used only to run Gradle.
    toolchain { languageVersion.set(JavaLanguageVersion.of(21)) }
    withSourcesJar()
}

base { archivesName.set(artifactName) }
repositories { mavenCentral() }

dependencies {
    compileOnly("org.scala-lang:scala-library:$scalaVersion")
    compileOnly("org.apache.spark:spark-sql_$scalaBinaryVersion:$sparkVersion")
    // Jackson is supplied by Spark at runtime; do not force a competing version on consumers.
    compileOnly("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")

    testImplementation("org.scala-lang:scala-library:$scalaVersion")
    testImplementation("org.apache.spark:spark-sql_$scalaBinaryVersion:$sparkVersion")
    testImplementation("io.delta:delta-spark_$scalaBinaryVersion:$deltaVersion")
    testImplementation("org.scalatest:scalatest_$scalaBinaryVersion:$scalatestVersion")
    testImplementation("com.vladsch.flexmark:flexmark-all:0.64.8")
}

tasks.withType<ScalaCompile>().configureEach {
    scalaCompileOptions.additionalParameters = listOf("-release", "17", "-deprecation", "-feature", "-unchecked")
}

val testSuites = listOf(
    "com.recon.engine.BasicReconSpec",
    "com.recon.engine.ActionSinkIntegrationSpec",
    "com.recon.engine.DeepNestedStressSpec" // The new Ultimate Stress Test
)

val scalatest = tasks.register<JavaExec>("scalatest") {
    dependsOn(tasks.testClasses)
    mainClass.set("org.scalatest.tools.Runner")
    classpath = sourceSets["test"].runtimeClasspath
    
    jvmArgs = listOf(
        "-Xms512m", "-Xmx1536m",
        "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED", "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED", "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )
    val runArgs = mutableListOf("-oDF", "-h", layout.buildDirectory.dir("reports/scalatest").get().asFile.absolutePath) 
    testSuites.forEach { suite -> runArgs.add("-s"); runArgs.add(suite) }
    args = runArgs
}

tasks.withType<Test> { failOnNoDiscoveredTests = false }
tasks.named("test") { dependsOn(scalatest) }
tasks.check { dependsOn(scalatest) }

publishing {
    publications {
        create<MavenPublication>("mavenScala") {
            from(components["java"])
            artifactId = artifactName
            pom {
                name.set("Spark Recon Engine")
                description.set("A Scala library for reconciling two Apache Spark DataFrames.")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                        distribution.set("repo")
                    }
                }
            }
        }
    }
}
