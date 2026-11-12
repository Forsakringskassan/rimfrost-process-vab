# Rimfrost vård av boskap

Ett första exempel av en process, vård av boskap.

Build it with `./mvnw -s settings.xml clean verify`.

A GitHub workflow will also create a Docker image, it is published to [repository](https://github.com/Forsakringskassan/repository). It can be started with:

```sh
docker run -d \
  -p 8080:8080 \
  ghcr.io/forsakringskassan/rimfrost-vard-av-boskap-app:snapshot
```
## Processvy

Processen har en processvy (stegindikatorn) som visar handläggaren var i processen en uppgift befinner sig. Processvy-länken `processvy.url=/process/vab` i `application.properties` skickas med i regelanropen, och [processbeskrivning.json](src/main/resources/processbeskrivning.json) bredvid `vab.bpmn` listar processens steg och vilka aktivitets-ID:n som hör till varje steg. Stegen hämtas med `GET /processvy/steg?aktivitetId={uuid}`. Formatet beskrivs i [rimfrost-framework-process](https://github.com/Forsakringskassan/rimfrost-framework-process#processvy).

Stegen beskrivs också i dokumentations-BPMN:en [docs/vab-processbeskrivning.bpmn](docs/vab-processbeskrivning.bpmn), som inte körs. Varje task är ett steg med rubrik, beskrivning och de aktivitets-ID:n som hör till steget. Processbeskrivningen ska på sikt genereras från den, och tills dess hålls de lika för hand.

Den som ändrar aktiviteterna i `vab.bpmn` uppdaterar även processbeskrivningen och dokumentations-BPMN:en. `VabProcessbeskrivningTest` går sönder om ett aktivitets-ID i BPMN-filen saknas i processbeskrivningen. Kraven finns i [docs/krav.md](docs/krav.md).

`maven-ci` validerar `processbeskrivning.json` mot ramverkets JSON Schema genom att anropa workflowet `validate-processbeskrivning.yaml` i rimfrost-framework-process. Schemat kontrollerar fälten och att inga andra fält finns, att aktivitets-ID:n är UUID, att ett steg har antingen `steg` eller `aktiviteter` och att det finns högst tre nivåer. Unika id och att ett aktivitets-ID bara hör till ett steg kontrolleras när processen startar. Ange samma version av ramverket i `uses:` och `framework-ref` som i `pom.xml`.

## Testing the docker image

src/test contains a test setup using Java Testcontainers (https://java.testcontainers.org/)<br>
The test launches a kafka broker and vab as test containers and mocks the RTF kafka interactions.
The test uses DTOs generated from OpenAPI- and AsyncAPI-specifications.

Run tests with `./mvnw -s settings.xml clean verify`.

See also: [fk-maven](https://github.com/Forsakringskassan/fk-maven).
