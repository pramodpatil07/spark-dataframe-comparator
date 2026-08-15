plugins { scala; `maven-publish` }

group = "org.ind.icon.data"
version = "1.0.0-SNAPSHOT"

val scalaVersion: String by project
val scalaBinaryVersion: String by project
val sparkVersion: String by project
val scalatestVersion: String by project
val deltaVersion : String by project 

java { toolchain { languageVersion.set(JavaLanguageVersion.of(21)) } }
repositories { mavenCentral() }

dependencies {
    compileOnly("org.scala-lang:scala-library:$scalaVersion")
    compileOnly("org.apache.spark:spark-sql_$scalaBinaryVersion:$sparkVersion")
    compileOnly("io.delta:delta-spark_$scalaBinaryVersion:$deltaVersion")

    testImplementation("org.scala-lang:scala-library:$scalaVersion")
    testImplementation("org.apache.spark:spark-sql_$scalaBinaryVersion:$sparkVersion")
    testImplementation("io.delta:delta-spark_$scalaBinaryVersion:$deltaVersion")
    testImplementation("org.scalatest:scalatest_$scalaBinaryVersion:$scalatestVersion")
    testImplementation("com.vladsch.flexmark:flexmark-all:0.64.8")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.2")
    implementation("com.fasterxml.jackson.module:jackson-module-scala_$scalaBinaryVersion:2.15.2")
}

val testSuites = listOf(
    "org.ind.icon.data.comparator.BasicComparatorSpec",
    "org.ind.icon.data.comparator.ActionSinkIntegrationSpec",
    "org.ind.icon.data.comparator.DeepNestedStressSpec"
)

val scalatest by tasks.registering(JavaExec::class) {
    dependsOn(tasks.testClasses)
    mainClass.set("org.scalatest.tools.Runner")
    classpath = sourceSets["test"].runtimeClasspath
    
    jvmArgs = listOf(
        "-Xms2048m", "-Xmx4096m",
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
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            groupId = project.group.toString()
            artifactId = "${project.name}_$scalaBinaryVersion"
            version = project.version.toString()
        }
    }
}