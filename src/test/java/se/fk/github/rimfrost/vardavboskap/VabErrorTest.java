package se.fk.github.rimfrost.vardavboskap;

import io.quarkus.test.junit.QuarkusTest;
import org.jboss.logmanager.ExtHandler;
import org.jboss.logmanager.ExtLogRecord;
import org.jboss.logmanager.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.fk.rimfrost.HandlaggningResponseMessagePayload;
import se.fk.rimfrost.framework.regel.RegelRequestMessagePayload;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class VabErrorTest extends VabTestBase
{
   private final List<String> capturedLogs = new CopyOnWriteArrayList<>();

   @BeforeAll
   void setupLogCapture()
   {
      Logger.getLogger("").addHandler(new ExtHandler()
      {
         @Override
         protected void doPublish(ExtLogRecord record)
         {
            capturedLogs.add(record.getLevel().getName() + " " + record.getFormattedMessage());
         }

         @Override
         public void flush()
         {
         }

         @Override
         public void close() throws SecurityException
         {
         }
      });
   }

   @AfterEach
   void clearLogs()
   {
      capturedLogs.clear();
   }

   /**
    * Verifies the "Avsluta process med error" path when maskinell kontroll returns a technical error. The process must
    * skip manuell kontroll and bekräfta beslut and instead send a handlaggning response with the error populated.
    */
   @Test
   void testVabMaskinellError() throws Exception
   {
      var handlaggningId = UUID.randomUUID().toString();
      System.out.println("Starting testVabMaskinellError");

      CompletableFuture<Void> responderRtfMaskinell = startKafkaResponderWithError(
            rtfMaskinellRequestTopic, rtfMaskinellResponseTopic, "RTF-001", "Tekniskt fel i rtfMaskinell",
            handlaggningId);

      sendVabHandlaggningRequest(handlaggningId, "A1");

      String rtfMaskinellRequest = readKafkaRequestMessage(rtfMaskinellRequestTopic, handlaggningId);
      System.out.println("Received rtfMaskinellRequest: " + rtfMaskinellRequest);
      RegelRequestMessagePayload rtfMaskinellRequestMessagePayload = mapper.readValue(rtfMaskinellRequest,
            RegelRequestMessagePayload.class);
      assertEquals(handlaggningId, rtfMaskinellRequestMessagePayload.getData().getHandlaggningId());
      assertEquals("d4ab4820-68d9-41e0-abe1-cd8f9865d275", rtfMaskinellRequestMessagePayload.getData().getAktivitetId());

      responderRtfMaskinell.get(topicTimeout, TimeUnit.SECONDS);

      String vabHandlaggningResponse = readKafkaRequestMessage(handlaggningResponseTopic, handlaggningId);
      System.out.println("Received vabHandlaggningResponse: " + vabHandlaggningResponse);
      HandlaggningResponseMessagePayload response = mapper.readValue(vabHandlaggningResponse,
            HandlaggningResponseMessagePayload.class);
      assertEquals(handlaggningId, response.getData().getHandlaggningId());
      assertEquals("FEL", response.getData().getResultat());
      assertNotNull(response.getData().getError());

      boolean errorLogged = capturedLogs.stream()
            .filter(line -> line.contains(handlaggningId))
            .anyMatch(line -> line.contains("ERROR") && line.contains("RTF-001"));
      assertTrue(errorLogged, "Expected ERROR log containing handlaggningId and felkod RTF-001");
   }
}
