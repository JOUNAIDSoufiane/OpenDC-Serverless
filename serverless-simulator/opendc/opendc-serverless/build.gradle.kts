/*
 * MIT License
 *
 * Copyright (c) 2019 atlarge-research
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

description = "Function-as-a-Service platform simulator for OpenDC"

/* Build configuration */
plugins {
    `kotlin-library-convention`
}

dependencies {
    api(project(":opendc:opendc-core"))
    api(project(":opendc:opendc-compute"))
    api(project(":opendc:opendc-format"))
    api("org.apache.commons:commons-csv:1.8")
    api("com.fasterxml.jackson.module:jackson-module-kotlin:2.9.8")
    implementation(files("$projectDir/jar-dependencies/REngine.jar"))
    implementation(files("$projectDir/jar-dependencies/Rserve.jar"))
    implementation("me.tongfei:progressbar:0.8.1")
    implementation("org.apache.commons:commons-math3:3.6.1")
    implementation("com.xenomachina:kotlin-argparser:2.0.7")
    implementation("org.apache.parquet:parquet-avro:1.11.0")
    implementation("org.apache.hadoop:hadoop-client:3.2.1") {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
    implementation(kotlin("stdlib"))
    implementation(kotlin("reflect"))


    testImplementation("org.junit.jupiter:junit-jupiter-api:${Library.JUNIT_JUPITER}")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:${Library.JUNIT_JUPITER}")
    testImplementation("org.junit.platform:junit-platform-launcher:${Library.JUNIT_PLATFORM}")
}
