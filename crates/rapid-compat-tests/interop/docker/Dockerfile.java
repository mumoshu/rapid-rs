FROM eclipse-temurin:21-jre

# The standalone-agent.jar is built outside the container via
# `mvn package` in references/rapid-java and copied in by the
# docker-compose context.
COPY standalone-agent.jar /app/standalone-agent.jar

ENTRYPOINT ["java", \
    "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED", \
    "--add-opens", "java.base/java.nio=ALL-UNNAMED", \
    "-jar", "/app/standalone-agent.jar"]
