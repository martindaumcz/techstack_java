plugins {
	java
	// Note: Plugin versions must be literal strings due to Gradle restrictions
	// Update these manually when needed
	id("org.springframework.boot") version "3.5.7"
	id("io.spring.dependency-management") version "1.1.7"
}

group = "org.mdaum"
version = "0.0.1-SNAPSHOT"
description = "Spring Boot Tech Stack project"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(21)
	}
}

repositories {
	mavenCentral()
}

// Load dependency versions from gradle.properties
val netflixDgsVersion: String by project
val awsSdkVersion: String by project
val awsSdkRegionsVersion: String by project
val reactorKafkaVersion: String by project

dependencies {
	implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    //implementation("org.springframework.boot:spring-boot-starter-web")
	implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.kafka:spring-kafka")
	implementation("com.netflix.graphql.dgs:graphql-dgs-spring-graphql-starter")
	//implementation("org.flywaydb:flyway-core")
	//implementation("org.flywaydb:flyway-database-postgresql")
	runtimeOnly("org.postgresql:postgresql")
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("com.netflix.graphql.dgs:graphql-dgs-spring-graphql-starter-test")
	testImplementation("io.projectreactor:reactor-test")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    //aws sdk
    implementation("software.amazon.awssdk:regions:$awsSdkRegionsVersion")
    implementation("software.amazon.awssdk:sqs:$awsSdkVersion")
    implementation("software.amazon.awssdk:s3:$awsSdkVersion")
    implementation("software.amazon.awssdk:s3-transfer-manager:$awsSdkVersion")

    //reactor and kafka
    implementation("io.projectreactor.kafka:reactor-kafka:$reactorKafkaVersion")
}

dependencyManagement {
	imports {
		mavenBom("com.netflix.graphql.dgs:graphql-dgs-platform-dependencies:$netflixDgsVersion")
	}
}

tasks.withType<Test> {
	useJUnitPlatform()
}
