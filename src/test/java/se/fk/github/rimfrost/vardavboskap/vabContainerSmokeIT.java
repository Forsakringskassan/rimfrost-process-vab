package se.fk.github.rimfrost.vardavboskap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import se.fk.rimfrost.SpecVersion;
import se.fk.rimfrost.VahKundbehovsflodeRequestMessageData;
import se.fk.rimfrost.VahKundbehovsflodeRequestMessagePayload;
import se.fk.rimfrost.VahKundbehovsflodeResponseMessagePayload;
import se.fk.rimfrost.framework.regel.RegelRequestMessagePayload;
import se.fk.rimfrost.framework.regel.RegelRequestMessagePayloadData;
import se.fk.rimfrost.framework.regel.RegelResponseMessagePayload;
import se.fk.rimfrost.framework.regel.RegelResponseMessagePayloadData;
import se.fk.rimfrost.framework.regel.Utfall;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("deprecation")
@Testcontainers
public class vabContainerSmokeIT
{

   private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
         .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
   private static KafkaContainer kafka;
   private static GenericContainer<?> vab;
   private static final String kafkaImage = TestConfig.get("kafka.image");
   private static final String vabImage = TestConfig.get("vab.image");
   private static final int vabPort = TestConfig.getInt("vab.port");
   private static final String vabKundbehovsflodeRequestTopic = TestConfig.get("vab.kundbehovsflode.requests.topic");
   private static final String vabKundbehovsflodeResponseTopic = TestConfig.get("vab.kundbehovsflode.responses.topic");
   private static final String beraknaersattningRequestTopic = TestConfig.get("rtf.maskinell.requests.topic");
   private static final String beraknaersattningResponseTopic = TestConfig.get("rtf.maskinell.responses.topic");
   private static final String rtfManuellRequestTopic = TestConfig.get("rtf.manuell.requests.topic");
   private static final String rtfManuellResponseTopic = TestConfig.get("rtf.manuell.responses.topic");
   private static final String bekraftaBeslutRequestTopic = TestConfig.get("bekraftabeslut.requests.topic");
   private static final String bekraftaBeslutResponseTopic = TestConfig.get("bekraftabeslut.responses.topic");
   private static final int topicTimeout = TestConfig.getInt("topic.timeout");
   private static final String networkAlias = TestConfig.get("network.alias");
   private static final String smallryeKafkaBootstrapServers = networkAlias + ":9092";
   private static Network network = Network.newNetwork();

   @BeforeAll
   static void setupKafka()
   {

      kafka = new KafkaContainer(DockerImageName.parse(kafkaImage)
            .asCompatibleSubstituteFor("apache/kafka"))
            .withNetwork(network)
            .withNetworkAliases(networkAlias);
      kafka.start();
      try
      {
         createTopic(vabKundbehovsflodeRequestTopic, 1, (short) 1);
         createTopic(vabKundbehovsflodeResponseTopic, 1, (short) 1);
         createTopic(beraknaersattningRequestTopic, 1, (short) 1);
         createTopic(beraknaersattningResponseTopic, 1, (short) 1);
         createTopic(rtfManuellRequestTopic, 1, (short) 1);
         createTopic(rtfManuellResponseTopic, 1, (short) 1);
      }
      catch (Exception e)
      {
         throw new RuntimeException("Failed to create Kafka topics", e);
      }
      setupvab();

   }

   static void setupvab()
   {
      //noinspection resource
      vab = new GenericContainer<>(DockerImageName.parse(vabImage))
            .withNetwork(network)
            .withEnv("MP_MESSAGING_CONNECTOR_SMALLRYE_KAFKA_BOOTSTRAP_SERVERS", smallryeKafkaBootstrapServers);
      vab.start();
   }

   static void createTopic(String topicName, int numPartitions, short replicationFactor) throws Exception
   {
      String bootstrap = kafka.getBootstrapServers().replace("PLAINTEXT://", "");
      Properties props = new Properties();
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);

      try (AdminClient admin = AdminClient.create(props))
      {
         NewTopic topic = new NewTopic(topicName, numPartitions, replicationFactor);
         admin.createTopics(List.of(topic)).all().get();
         System.out.printf("Created topic: %S%n", topicName);
      }
   }

   @AfterAll
   static void tearDown()
   {
      if (vab != null)
         vab.stop();
      if (kafka != null)
         kafka.stop();
   }

   private String readKafkaRequestMessage(String topic)
   {
      String bootstrap = kafka.getBootstrapServers().replace("PLAINTEXT://", "");
      Properties props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.currentTimeMillis());
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

      try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props))
      {
         System.out.printf("New kafka consumer subscribing to topic: %s%n", topic);
         consumer.subscribe(Collections.singletonList(topic));
         ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(120));

         if (records.isEmpty())
         {
            throw new IllegalStateException("No Kafka message received on topic " + topic);
         }
         return records.iterator().next().value();
      }
   }

   private CompletableFuture<Void> startKafkaResponder(String requesttopic, String responseTopic, Utfall utfall)
   {
      return CompletableFuture.runAsync(() -> {
         try (KafkaConsumer<String, String> consumer = createConsumer())
         {
            consumer.subscribe(Collections.singletonList(requesttopic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            if (records.isEmpty())
            {
               throw new IllegalStateException("No Kafka message received on " + requesttopic);
            }

            // Deserialize request message into typed payload
            String message = records.iterator().next().value();
            RegelRequestMessagePayload request = mapper.readValue(message, RegelRequestMessagePayload.class);
            // Extract data safely
            RegelRequestMessagePayloadData requestData = request.getData();
            if (requestData == null)
            {
               throw new IllegalStateException("Missing data field in Kafka message: " + message);
            }
            String kundbehovsflodeId = requestData.getKundbehovsflodeId();
            // Create typed response data object
            RegelResponseMessagePayloadData responseData = new RegelResponseMessagePayloadData();
            responseData.setKundbehovsflodeId(kundbehovsflodeId);
            responseData.setUtfall(utfall);

            sendRegelResponse(request, responseTopic, responseData);
            System.out.printf("Sent mock Kafka response for kundbehovsflodeId=%s%n on topic %s", kundbehovsflodeId,
                  responseTopic);
         }
         catch (Exception e)
         {
            throw new RuntimeException("Kafka responder failed", e);
         }
      }, Executors.newSingleThreadExecutor());
   }

   private void sendvabKundbehovsflodeRequest(String kundbehovsflodeId, String messageKey) throws Exception
   {
      VahKundbehovsflodeRequestMessagePayload payload = new VahKundbehovsflodeRequestMessagePayload();
      VahKundbehovsflodeRequestMessageData data = new VahKundbehovsflodeRequestMessageData();
      data.setKundbehovsflodeId(kundbehovsflodeId);
      payload.setSpecversion(SpecVersion.NUMBER_1_DOT_0);
      payload.setId("TestId-001");
      payload.setSource("TestSource-001");
      payload.setType(vabKundbehovsflodeRequestTopic);
      payload.setData(data);
      // Serialize entire payload to JSON
      String eventJson = mapper.writeValueAsString(payload);

      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

      try (KafkaProducer<String, String> producer = new KafkaProducer<>(props))
      {
         ProducerRecord<String, String> record = new ProducerRecord<>(
               vabKundbehovsflodeRequestTopic,
               messageKey,
               eventJson);
         System.out.printf("Kafka sending to topic : %s, json: %s%n", vabKundbehovsflodeRequestTopic, eventJson);
         producer.send(record).get();
      }
   }

   private void sendRegelResponse(RegelRequestMessagePayload request,
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

      // Serialize entire payload to JSON
      String eventJson = mapper.writeValueAsString(payload);

      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

      try (KafkaProducer<String, String> producer = new KafkaProducer<>(props))
      {
         ProducerRecord<String, String> record = new ProducerRecord<>(
               topic,
               request.getId(), // message key
               eventJson);
         System.out.printf("Kafka mock sending: %s\n", eventJson);
         producer.send(record).get();
      }
   }

   private KafkaConsumer<String, String> createConsumer()
   {
      Properties props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.currentTimeMillis());
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      return new KafkaConsumer<>(props);
   }

   @Test
   void TestvabSmoke() throws Exception
   {
      var kundbehovsflodeId = UUID.randomUUID().toString();
      System.out.println("Starting TestvabSmoke");
      // Start background Kafka responders
      CompletableFuture<Void> responderberaknaersattning = startKafkaResponder(beraknaersattningRequestTopic,
            beraknaersattningResponseTopic,
            Utfall.UTREDNING);
      CompletableFuture<Void> responderRtfManuell = startKafkaResponder(rtfManuellRequestTopic, rtfManuellResponseTopic,
            Utfall.JA);
      CompletableFuture<Void> responderBekraftaBeslut = startKafkaResponder(bekraftaBeslutRequestTopic,
            bekraftaBeslutResponseTopic, Utfall.JA);
      // Send Kundbehovsflöde request to start workflow
      sendvabKundbehovsflodeRequest(kundbehovsflodeId, "A1");
      // Verify rtf maskinell message produced by vab
      String beraknaersattningRequest = readKafkaRequestMessage(beraknaersattningRequestTopic);
      System.out.println("Received beraknaersattningRequest: " + beraknaersattningRequest);
      RegelRequestMessagePayload beraknaersattningRequestMessagePayload = mapper.readValue(beraknaersattningRequest,
            RegelRequestMessagePayload.class);
      assertEquals(kundbehovsflodeId, beraknaersattningRequestMessagePayload.getData().getKundbehovsflodeId());
      // Wait for kafka responder to complete
      responderberaknaersattning.get(topicTimeout, TimeUnit.SECONDS);
      // Verify rtf manuell message produced by vab
      String rtfManuellRequest = readKafkaRequestMessage(rtfManuellRequestTopic);
      System.out.println("Received rtfManuellRequest: " + rtfManuellRequest);
      RegelRequestMessagePayload rtfManuellRequestMessagePayload = mapper.readValue(rtfManuellRequest,
            RegelRequestMessagePayload.class);
      assertEquals(kundbehovsflodeId, rtfManuellRequestMessagePayload.getData().getKundbehovsflodeId());

      // Wait for kafka responder to complete
      responderRtfManuell.get(topicTimeout, TimeUnit.SECONDS);

      // Verify bekraftaBeslut message produced by vab
      String bekraftaBeslutRequest = readKafkaRequestMessage(bekraftaBeslutRequestTopic);
      System.out.println("Received bekraftaBeslutRequest: " + bekraftaBeslutRequest);
      RegelRequestMessagePayload bekraftaBeslutRequestMessagePayload = mapper.readValue(bekraftaBeslutRequest,
            RegelRequestMessagePayload.class);
      assertEquals(kundbehovsflodeId, bekraftaBeslutRequestMessagePayload.getData().getKundbehovsflodeId());

      // Wait for kafka responder to complete
      responderBekraftaBeslut.get(topicTimeout, TimeUnit.SECONDS);

      // Wait for response from vab
      String vabKundbehovsflodeResponse = readKafkaRequestMessage(vabKundbehovsflodeResponseTopic);
      System.out.println("Received vabKundbehovsflodeResponse: " + vabKundbehovsflodeResponse);
      VahKundbehovsflodeResponseMessagePayload vabKundbehovsflodeRequestMessagePayload = mapper
            .readValue(vabKundbehovsflodeResponse, VahKundbehovsflodeResponseMessagePayload.class);
      assertEquals(kundbehovsflodeId, vabKundbehovsflodeRequestMessagePayload.getData().getKundbehovsflodeId());
      assertEquals("GODKÄND", vabKundbehovsflodeRequestMessagePayload.getData().getResultat());
   }
}
