package se.fk.github.rimfrost.vardavboskap;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import se.fk.rimfrost.HandlaggningResponseMessagePayload;
import se.fk.rimfrost.framework.regel.RegelRequestMessagePayload;
import se.fk.rimfrost.framework.regel.Utfall;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
class VabSmokeTest extends VabTestBase
{
   @Test
   void testVabSmoke() throws Exception
   {
      var handlaggningId = UUID.randomUUID().toString();
      System.out.println("Starting testVabSmoke");

      CompletableFuture<Void> responderRtfMaskinell = startKafkaResponder(rtfMaskinellRequestTopic,
            rtfMaskinellResponseTopic, Utfall.UTREDNING, handlaggningId);
      CompletableFuture<Void> responderRtfManuell = startKafkaResponder(rtfManuellRequestTopic,
            rtfManuellResponseTopic, Utfall.JA, handlaggningId);
      CompletableFuture<Void> responderBekraftaBeslut = startKafkaResponder(bekraftaBeslutRequestTopic,
            bekraftaBeslutResponseTopic, Utfall.JA, handlaggningId);

      sendVabHandlaggningRequest(handlaggningId, "A1");

      String rtfMaskinellRequest = readKafkaRequestMessage(rtfMaskinellRequestTopic, handlaggningId);
      System.out.println("Received rtfMaskinellRequest: " + rtfMaskinellRequest);
      RegelRequestMessagePayload rtfMaskinellRequestMessagePayload = mapper.readValue(rtfMaskinellRequest,
            RegelRequestMessagePayload.class);
      assertEquals(handlaggningId, rtfMaskinellRequestMessagePayload.getData().getHandlaggningId());
      assertEquals("d4ab4820-68d9-41e0-abe1-cd8f9865d275", rtfMaskinellRequestMessagePayload.getData().getAktivitetId());

      responderRtfMaskinell.get(topicTimeout, TimeUnit.SECONDS);

      String rtfManuellRequest = readKafkaRequestMessage(rtfManuellRequestTopic, handlaggningId);
      System.out.println("Received rtfManuellRequest: " + rtfManuellRequest);
      RegelRequestMessagePayload rtfManuellRequestMessagePayload = mapper.readValue(rtfManuellRequest,
            RegelRequestMessagePayload.class);
      assertEquals(handlaggningId, rtfManuellRequestMessagePayload.getData().getHandlaggningId());
      assertEquals("c58dd666-b3c1-4a30-91b8-76c3495668c6", rtfManuellRequestMessagePayload.getData().getAktivitetId());

      responderRtfManuell.get(topicTimeout, TimeUnit.SECONDS);

      String bekraftaBeslutRequest = readKafkaRequestMessage(bekraftaBeslutRequestTopic, handlaggningId);
      System.out.println("Received bekraftaBeslutRequest: " + bekraftaBeslutRequest);
      RegelRequestMessagePayload bekraftaBeslutRequestMessagePayload = mapper.readValue(bekraftaBeslutRequest,
            RegelRequestMessagePayload.class);
      assertEquals(handlaggningId, bekraftaBeslutRequestMessagePayload.getData().getHandlaggningId());
      assertEquals("8cde2355-aea5-4951-916f-08319b2f1e99", bekraftaBeslutRequestMessagePayload.getData().getAktivitetId());

      responderBekraftaBeslut.get(topicTimeout, TimeUnit.SECONDS);

      String vabHandlaggningResponse = readKafkaRequestMessage(handlaggningResponseTopic, handlaggningId);
      System.out.println("Received vabHandlaggningResponse: " + vabHandlaggningResponse);
      HandlaggningResponseMessagePayload handlaggningResponseMessagePayload = mapper.readValue(vabHandlaggningResponse,
            HandlaggningResponseMessagePayload.class);
      assertEquals(handlaggningId, handlaggningResponseMessagePayload.getData().getHandlaggningId());
      assertEquals("GODKÄND", handlaggningResponseMessagePayload.getData().getResultat());
   }

   @Test
   void testVabMaskinellJa() throws Exception
   {
      var handlaggningId = UUID.randomUUID().toString();
      System.out.println("Starting testVabMaskinellJa");

      CompletableFuture<Void> responderRtfMaskinell = startKafkaResponder(rtfMaskinellRequestTopic,
            rtfMaskinellResponseTopic, Utfall.JA, handlaggningId);
      CompletableFuture<Void> responderBekraftaBeslut = startKafkaResponder(bekraftaBeslutRequestTopic,
            bekraftaBeslutResponseTopic, Utfall.JA, handlaggningId);

      sendVabHandlaggningRequest(handlaggningId, "A1");

      String rtfMaskinellRequest = readKafkaRequestMessage(rtfMaskinellRequestTopic, handlaggningId);
      System.out.println("Received rtfMaskinellRequest: " + rtfMaskinellRequest);
      RegelRequestMessagePayload maskinellPayload = mapper.readValue(rtfMaskinellRequest,
            RegelRequestMessagePayload.class);
      assertEquals(handlaggningId, maskinellPayload.getData().getHandlaggningId());
      assertEquals("d4ab4820-68d9-41e0-abe1-cd8f9865d275", maskinellPayload.getData().getAktivitetId());

      responderRtfMaskinell.get(topicTimeout, TimeUnit.SECONDS);

      // Manuell is skipped — process goes directly to bekraftaBeslut
      String bekraftaBeslutRequest = readKafkaRequestMessage(bekraftaBeslutRequestTopic, handlaggningId);
      System.out.println("Received bekraftaBeslutRequest: " + bekraftaBeslutRequest);
      RegelRequestMessagePayload bekraftaPayload = mapper.readValue(bekraftaBeslutRequest,
            RegelRequestMessagePayload.class);
      assertEquals(handlaggningId, bekraftaPayload.getData().getHandlaggningId());
      assertEquals("8cde2355-aea5-4951-916f-08319b2f1e99", bekraftaPayload.getData().getAktivitetId());

      responderBekraftaBeslut.get(topicTimeout, TimeUnit.SECONDS);

      String vabHandlaggningResponse = readKafkaRequestMessage(handlaggningResponseTopic, handlaggningId);
      System.out.println("Received vabHandlaggningResponse: " + vabHandlaggningResponse);
      HandlaggningResponseMessagePayload response = mapper.readValue(vabHandlaggningResponse,
            HandlaggningResponseMessagePayload.class);
      assertEquals(handlaggningId, response.getData().getHandlaggningId());
      assertEquals("GODKÄND", response.getData().getResultat());
   }
}
