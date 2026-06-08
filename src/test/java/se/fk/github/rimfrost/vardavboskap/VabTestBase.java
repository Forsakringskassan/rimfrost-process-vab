package se.fk.github.rimfrost.vardavboskap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.TestInstance;
import se.fk.rimfrost.HandlaggningRequestMessageData;
import se.fk.rimfrost.HandlaggningRequestMessagePayload;
import se.fk.rimfrost.SpecVersion;
import se.fk.rimfrost.framework.regel.RegelErrorInformation;
import se.fk.rimfrost.framework.regel.RegelRequestMessagePayload;
import se.fk.rimfrost.framework.regel.RegelRequestMessagePayloadData;
import se.fk.rimfrost.framework.regel.RegelResponseMessagePayload;
import se.fk.rimfrost.framework.regel.RegelResponseMessagePayloadData;
import se.fk.rimfrost.framework.regel.Utfall;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class VabTestBase
{
   protected static final ObjectMapper mapper = new ObjectMapper()
         .registerModule(new JavaTimeModule())
         .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

   protected static final String vabHandlaggningRequestTopic = "vab-handlaggning-requests";
   protected static final String handlaggningResponseTopic = "handlaggning-responses";
   protected static final String rtfMaskinellRequestTopic = "rtf-maskinell-requests";
   protected static final String rtfMaskinellResponseTopic = "rtf-maskinell-responses";
   protected static final String rtfManuellRequestTopic = "rtf-manuell-requests";
   protected static final String rtfManuellResponseTopic = "rtf-manuell-responses";
   protected static final String bekraftaBeslutRequestTopic = "bekraftabeslut-requests";
   protected static final String bekraftaBeslutResponseTopic = "bekraftabeslut-responses";
   protected static final int topicTimeout = 20;

   @ConfigProperty(name = "kafka.bootstrap.servers")
   protected String bootstrapServers;

   /**
    * Reads the first message from {@code topic} whose {@code data.handlaggningId} matches the given value. Skips
    * messages left over from other test runs sharing the same topics. Polls repeatedly until a match is found or the
    * 120-second deadline expires.
    */
   protected String readKafkaRequestMessage(String topic, String expectedHandlaggningId)
   {
      Properties props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

      long deadline = System.currentTimeMillis() + Duration.ofSeconds(120).toMillis();
      try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props))
      {
         System.out.printf("New kafka consumer subscribing to topic: %s%n", topic);
         consumer.subscribe(Collections.singletonList(topic));
         while (System.currentTimeMillis() < deadline)
         {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (var record : records)
            {
               if (record.value().contains(expectedHandlaggningId))
               {
                  return record.value();
               }
            }
         }
         throw new IllegalStateException(
               "No Kafka message for handlaggningId " + expectedHandlaggningId + " received on topic " + topic);
      }
   }

   protected CompletableFuture<Void> startKafkaResponder(String requestTopic, String responseTopic, Utfall utfall,
         String expectedHandlaggningId)
   {
      return CompletableFuture.runAsync(() -> {
         long deadline = System.currentTimeMillis() + Duration.ofSeconds(topicTimeout).toMillis();
         try (KafkaConsumer<String, String> consumer = createConsumer())
         {
            consumer.subscribe(Collections.singletonList(requestTopic));
            while (System.currentTimeMillis() < deadline)
            {
               ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
               for (var record : records)
               {
                  if (!record.value().contains(expectedHandlaggningId))
                  {
                     continue;
                  }
                  RegelRequestMessagePayload request = mapper.readValue(record.value(),
                        RegelRequestMessagePayload.class);
                  RegelRequestMessagePayloadData requestData = request.getData();
                  if (requestData == null)
                  {
                     throw new IllegalStateException("Missing data field in Kafka message: " + record.value());
                  }
                  RegelResponseMessagePayloadData responseData = new RegelResponseMessagePayloadData();
                  responseData.setHandlaggningId(requestData.getHandlaggningId());
                  responseData.setUtfall(utfall);
                  sendRegelResponse(request, responseTopic, responseData);
                  System.out.printf("Sent mock Kafka response for handlaggningId=%s on topic %s%n",
                        requestData.getHandlaggningId(), responseTopic);
                  return;
               }
            }
            throw new IllegalStateException(
                  "No Kafka message for handlaggningId " + expectedHandlaggningId + " received on " + requestTopic);
         }
         catch (Exception e)
         {
            throw new RuntimeException("Kafka responder failed", e);
         }
      }, Executors.newSingleThreadExecutor());
   }

   /**
    * Starts a background responder that listens on {@code requestTopic} and replies with an error response on
    * {@code responseTopic}. Runs on a separate thread so the main test thread can concurrently send requests and read
    * results without deadlocking. Exceptions thrown inside are re-raised when the returned future is joined with
    * {@code .get()}.
    */
   protected CompletableFuture<Void> startKafkaResponderWithError(String requestTopic, String responseTopic,
         String felkod, String felmeddelande, String expectedHandlaggningId)
   {
      return CompletableFuture.runAsync(() -> {
         long deadline = System.currentTimeMillis() + Duration.ofSeconds(topicTimeout).toMillis();
         try (KafkaConsumer<String, String> consumer = createConsumer())
         {
            consumer.subscribe(Collections.singletonList(requestTopic));
            while (System.currentTimeMillis() < deadline)
            {
               ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
               for (var record : records)
               {
                  if (!record.value().contains(expectedHandlaggningId))
                  {
                     continue;
                  }
                  RegelRequestMessagePayload request = mapper.readValue(record.value(),
                        RegelRequestMessagePayload.class);
                  RegelRequestMessagePayloadData requestData = request.getData();
                  if (requestData == null)
                  {
                     throw new IllegalStateException("Missing data field in Kafka message: " + record.value());
                  }

                  RegelErrorInformation error = new RegelErrorInformation();
                  error.setFelkod(felkod);
                  error.setFelmeddelande(felmeddelande);

                  RegelResponseMessagePayloadData responseData = new RegelResponseMessagePayloadData();
                  responseData.setHandlaggningId(requestData.getHandlaggningId());
                  responseData.setError(error);

                  sendRegelResponse(request, responseTopic, responseData);
                  System.out.printf("Sent error Kafka response for handlaggningId=%s on topic %s%n",
                        requestData.getHandlaggningId(), responseTopic);
                  return;
               }
            }
            throw new IllegalStateException(
                  "No Kafka message for handlaggningId " + expectedHandlaggningId + " received on " + requestTopic);
         }
         catch (Exception e)
         {
            throw new RuntimeException("Kafka responder failed", e);
         }
      }, Executors.newSingleThreadExecutor());
   }

   protected void sendVabHandlaggningRequest(String handlaggningId, String messageKey) throws Exception
   {
      HandlaggningRequestMessagePayload payload = new HandlaggningRequestMessagePayload();
      HandlaggningRequestMessageData data = new HandlaggningRequestMessageData();
      data.setHandlaggningId(handlaggningId);
      payload.setSpecversion(SpecVersion.V1);
      payload.setId("TestId-001");
      payload.setSource("TestSource-001");
      payload.setType(vabHandlaggningRequestTopic);
      payload.setData(data);
      String eventJson = mapper.writeValueAsString(payload);

      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

      try (KafkaProducer<String, String> producer = new KafkaProducer<>(props))
      {
         ProducerRecord<String, String> record = new ProducerRecord<>(
               vabHandlaggningRequestTopic,
               messageKey,
               eventJson);
         System.out.printf("Kafka sending to topic : %s, json: %s%n", vabHandlaggningRequestTopic, eventJson);
         producer.send(record).get();
      }
   }

   protected void sendRegelResponse(RegelRequestMessagePayload request,
         String topic,
         RegelResponseMessagePayloadData messageData) throws Exception
   {
      RegelResponseMessagePayload payload = new RegelResponseMessagePayload();
      payload.setSpecversion(request.getSpecversion());
      payload.setId(request.getId());
      payload.setSource(request.getSource());
      payload.setType(topic);
      payload.setTime(OffsetDateTime.now());
      payload.setKogitoparentprociid(request.getKogitoparentprociid());
      payload.setKogitorootprocid(request.getKogitorootprocid());
      payload.setKogitoproctype(request.getKogitoproctype());
      payload.setKogitoprocinstanceid(request.getKogitoprocinstanceid());
      payload.setKogitoprocist(request.getKogitoprocist());
      payload.setKogitoprocversion(request.getKogitoprocversion());
      payload.setKogitorootprociid(request.getKogitorootprociid());
      payload.setKogitoprocid(request.getKogitoprocid());
      payload.setKogitoprocrefid(request.getKogitoprocinstanceid());
      payload.setData(messageData);

      String eventJson = mapper.writeValueAsString(payload);

      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

      try (KafkaProducer<String, String> producer = new KafkaProducer<>(props))
      {
         ProducerRecord<String, String> record = new ProducerRecord<>(
               topic,
               request.getId(),
               eventJson);
         System.out.printf("Kafka mock sending: %s\n", eventJson);
         producer.send(record).get();
      }
   }

   protected KafkaConsumer<String, String> createConsumer()
   {
      Properties props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      return new KafkaConsumer<>(props);
   }
}
