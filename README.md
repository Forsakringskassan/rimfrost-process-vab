# Rimfrost vård av boskap

Ett första exempel av en process, vård av boskap.

Build it with `./mvnw -s settings.xml clean verify`.

A GitHub workflow will also create a Docker image, it is published to [repository](https://github.com/Forsakringskassan/repository). It can be started with:

```sh
docker run -d \
  -p 8080:8080 \
  ghcr.io/forsakringskassan/rimfrost-vard-av-boskap-app:snapshot
```
## Testing the docker image

src/test contains a test setup using Java Testcontainers (https://java.testcontainers.org/)<br>
The test launches a kafka broker and vab as test containers and mocks the RTF kafka interactions.
The test uses DTOs generated from OpenAPI- and AsyncAPI-specifications.

Run tests with `./mvnw -s settings.xml clean verify`.

See also: [fk-maven](https://github.com/Forsakringskassan/fk-maven).
