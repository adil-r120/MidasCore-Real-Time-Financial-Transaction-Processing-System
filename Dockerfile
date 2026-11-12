# Stage 1: Build stage
FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /app

# Cache dependencies
COPY pom.xml .
COPY .mvn .mvn
COPY mvnw .
COPY mvnw.cmd .
RUN ./mvnw dependency:go-offline -B || true

# Copy source code and build package
COPY src ./src
COPY application.yml .
RUN ./mvnw clean package -DskipTests

# Stage 2: Minimal Runtime stage
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app

# Create unprivileged application user for security
RUN addgroup --system spring && adduser --system spring --ingroup spring
USER spring:spring

# Copy built JAR from builder stage
COPY --from=builder /app/target/midas-core-1.0.0.jar app.jar
COPY application.yml .

EXPOSE 33400

ENTRYPOINT ["java", "-Djava.security.egd=file:/dev/./urandom", "-jar", "app.jar"]
