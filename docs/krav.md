# Krav — rimfrost-process-vab

## 1. Funktionella krav

### PVAB-FR-01 — Processbeskrivning

- **PVAB-FR-01.1** Processen ska ha en processbeskrivning bredvid `vab.bpmn` enligt FPROC-FR-02.1 i `rimfrost-framework-process`.
- **PVAB-FR-01.2** Processbeskrivningen ägs och underhålls av den som ritar och förvaltar BPMN:en.

### PVAB-FR-02 — Processvy-länk

- **PVAB-FR-02.1** Processens konfiguration ska innehålla länken `/process/vab` till processens mikrofrontend.

## 2. Icke-funktionella krav

### PVAB-NFR-01 — Testbarhet

- **PVAB-NFR-01.1** Ett test ska kontrollera att varje aktivitets-ID i `vab.bpmn` finns i processbeskrivningen (FPROC-NFR-01.1), så att bygget går sönder om de glider isär.
