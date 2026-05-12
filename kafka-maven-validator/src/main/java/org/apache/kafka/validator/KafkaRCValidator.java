package org.apache.kafka.validator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * Comprehensive Kafka Release Candidate Validator
 *
 * This class validates Apache Kafka release candidates by testing:
 * - Admin client operations (topics, configs, ACLs)
 * - Producer with various configurations (idempotent, transactional)
 * - Consumer with different commit strategies
 * - Consumer groups and rebalancing
 * - Kafka Streams basic topology
 * - Serialization/deserialization
 * - Error handling and recovery
 */
public class KafkaRCValidator {

    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_BLUE = "\u001B[34m";

    private static final int ADMIN_TIMEOUT_SECONDS = 10;
    private static final int CREATE_TIMEOUT_SECONDS = 30;
    private static final int PRODUCER_TIMEOUT_SECONDS = 5;
    private static final int CONSUMER_GROUP_TIMEOUT_SECONDS = 30;

    private final String bootstrapServers;
    private final String runId = UUID.randomUUID().toString().substring(0, 8);
    private final List<TestResult> results = new ArrayList<>();
    private final List<String> createdTopics = new ArrayList<>();

    public KafkaRCValidator(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    private String topicName(String base) {
        return base + "-" + runId;
    }

    public static void main(String[] args) {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        System.out.println("\n" + ANSI_BLUE + "=============================================" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "Kafka RC Maven Artifact Validator" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "Bootstrap Servers: " + bootstrapServers + ANSI_RESET);
        System.out.println(ANSI_BLUE + "=============================================" + ANSI_RESET + "\n");

        // Print Kafka client version
        try {
            String version = org.apache.kafka.common.utils.AppInfoParser.getVersion();
            System.out.println("Kafka Client Version: " + version);
            System.out.println("Kafka Commit ID: " + org.apache.kafka.common.utils.AppInfoParser.getCommitId());
        } catch (Exception e) {
            System.out.println("Could not determine Kafka version: " + e.getMessage());
        }

        KafkaRCValidator validator = new KafkaRCValidator(bootstrapServers);

        try {
            validator.runAllTests();
        } catch (Exception e) {
            System.err.println(ANSI_RED + "Fatal error during validation: " + e.getMessage() + ANSI_RESET);
            e.printStackTrace();
            System.exit(1);
        }

        validator.printSummary();

        int failures = (int) validator.results.stream().filter(r -> r.status == Status.FAIL).count();
        System.exit(failures > 0 ? 1 : 0);
    }

    public void runAllTests() throws Exception {
        Runnable[] tests = {
            this::testAdminClientOperations,
            this::testIdempotentProducer,
            this::testTransactionalProducer,
            this::testConsumerWithManualCommit,
            this::testConsumerWithAutoCommit,
            this::testConsumerGroupRebalancing,
            this::testPartitionAssignment,
            this::testCustomSerializers,
            this::testKafkaStreamsTopology,
            this::testMetricsAccess,
            this::testTopicDeletion,
        };

        try {
            for (Runnable test : tests) {
                try {
                    test.run();
                } catch (RuntimeException e) {
                    recordResult(e.getMessage(), Status.FAIL, e.getCause() != null ? e.getCause().getMessage() : "");
                }
            }
        } finally {
            cleanupTopics();
        }
    }

    // ==================== Admin Client Tests ====================

    private void testAdminClientOperations() {
        printSection("Admin Client Operations");

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        try (AdminClient admin = AdminClient.create(props)) {
            // Test 1: Cluster info
            testClusterInfo(admin);

            // Test 2: Create topic
            String testTopic = topicName("rc-validator-test-topic");
            testCreateTopic(admin, testTopic, 3, (short) 1);

            // Test 3: Describe topics
            testDescribeTopic(admin, testTopic);

            // Test 4: Alter topic config
            testAlterTopicConfig(admin, testTopic);

            // Test 5: List consumer groups
            testListConsumerGroups(admin, testTopic);

        } catch (Exception e) {
            recordResult("Admin Client", Status.FAIL, "Exception: " + e.getMessage());
        }
    }

    private void testClusterInfo(AdminClient admin) {
        try {
            DescribeClusterResult cluster = admin.describeCluster();
            String clusterId = cluster.clusterId().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            int nodeCount = cluster.nodes().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS).size();

            if (clusterId == null || clusterId.isBlank()) {
                recordResult("Get Cluster Info", Status.FAIL, "Cluster ID is empty");
            } else if (nodeCount < 1) {
                recordResult("Get Cluster Info", Status.FAIL,
                    String.format("Expected at least 1 node, got %d", nodeCount));
            } else {
                recordResult("Get Cluster Info", Status.PASS,
                    String.format("Cluster ID: %s, Nodes: %d", clusterId, nodeCount));
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            recordResult("Get Cluster Info", Status.FAIL, e.getMessage());
        }
    }

    private void testCreateTopic(AdminClient admin, String topicName, int partitions, short replicationFactor) {
        try {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            CreateTopicsResult result = admin.createTopics(Collections.singleton(newTopic));
            result.all().get(CREATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            createdTopics.add(topicName);
            recordResult("Create Topic", Status.PASS, "Created: " + topicName);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                // Verify existing topic matches expected config
                try {
                    TopicDescription desc = admin.describeTopics(Collections.singleton(topicName))
                        .topicNameValues().get(topicName).get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    int actualPartitions = desc.partitions().size();
                    int actualRF = desc.partitions().get(0).replicas().size();
                    if (actualPartitions == partitions && actualRF == replicationFactor) {
                        recordResult("Create Topic", Status.PASS,
                            String.format("Topic exists with correct config (partitions=%d, RF=%d): %s",
                                actualPartitions, actualRF, topicName));
                    } else {
                        recordResult("Create Topic", Status.FAIL,
                            String.format("Topic exists but config mismatch: expected partitions=%d RF=%d, got partitions=%d RF=%d",
                                partitions, replicationFactor, actualPartitions, actualRF));
                    }
                } catch (Exception descEx) {
                    recordResult("Create Topic", Status.WARN,
                        "Topic exists but could not verify config: " + descEx.getMessage());
                }
            } else {
                recordResult("Create Topic", Status.FAIL, e.getMessage());
            }
        } catch (InterruptedException | TimeoutException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            recordResult("Create Topic", Status.FAIL, e.getMessage());
        }
    }

    private void testDescribeTopic(AdminClient admin, String topicName) {
        try {
            DescribeTopicsResult result = admin.describeTopics(Collections.singleton(topicName));
            TopicDescription desc = result.topicNameValues().get(topicName).get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            int partitionCount = desc.partitions().size();
            if (partitionCount == 3) {
                recordResult("Describe Topic", Status.PASS,
                    String.format("Topic: %s, Partitions: %d (matches requested)", desc.name(), partitionCount));
            } else {
                recordResult("Describe Topic", Status.FAIL,
                    String.format("Topic: %s, Expected 3 partitions but got %d", desc.name(), partitionCount));
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            recordResult("Describe Topic", Status.FAIL, e.getMessage());
        }
    }

    private void testAlterTopicConfig(AdminClient admin, String topicName) {
        try {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            configs.put(resource, Collections.singleton(
                new AlterConfigOp(new ConfigEntry("retention.ms", "86400000"), AlterConfigOp.OpType.SET)
            ));

            admin.incrementalAlterConfigs(configs).all().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // Verify the config was applied by reading it back
            var descResult = admin.describeConfigs(Collections.singleton(resource));
            var config = descResult.all().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS).get(resource);
            String actualRetention = config.get("retention.ms").value();

            if ("86400000".equals(actualRetention)) {
                recordResult("Alter Topic Config", Status.PASS,
                    "Set and verified retention.ms=86400000");
            } else {
                recordResult("Alter Topic Config", Status.FAIL,
                    "Set retention.ms=86400000 but read back: " + actualRetention);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            recordResult("Alter Topic Config", Status.FAIL, e.getMessage());
        }
    }

    private void testListConsumerGroups(AdminClient admin, String topic) {
        try {
            // First, create a consumer group by consuming briefly
            String verifyGroupId = "rc-validator-list-groups-check-" + UUID.randomUUID();
            Properties cProps = new Properties();
            cProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            cProps.put(ConsumerConfig.GROUP_ID_CONFIG, verifyGroupId);
            cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            cProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cProps)) {
                consumer.subscribe(Collections.singleton(topic));
                consumer.poll(Duration.ofSeconds(2));
            }

            ListGroupsResult result = admin.listGroups();
            Collection<?> groups = result.all().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            boolean foundOurGroup = groups.stream()
                .map(Object::toString)
                .anyMatch(g -> g.contains(verifyGroupId));

            if (foundOurGroup) {
                recordResult("List Consumer Groups", Status.PASS,
                    String.format("Found %d groups, verified test group is listed", groups.size()));
            } else {
                recordResult("List Consumer Groups", Status.WARN,
                    String.format("Found %d groups, but test group not yet visible", groups.size()));
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            recordResult("List Consumer Groups", Status.FAIL, e.getMessage());
        }
    }

    // ==================== Producer Tests ====================

    private void testIdempotentProducer() {
        printSection("Idempotent Producer Test");

        String topic = topicName("rc-validator-idempotent-test");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Ensure topic exists
        createTopicIfNotExists(topic, 3, (short) 1);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<Future<RecordMetadata>> futures = new ArrayList<>();

            for (int i = 0; i < 100; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    topic, "key-" + i, "idempotent-value-" + i);
                futures.add(producer.send(record));
            }

            producer.flush();

            int successCount = 0;
            for (Future<RecordMetadata> future : futures) {
                try {
                    future.get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    successCount++;
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    // Count failures
                }
            }

            if (successCount == 100) {
                recordResult("Idempotent Producer Send", Status.PASS, "Sent 100/100 messages");
            } else {
                recordResult("Idempotent Producer Send", Status.WARN,
                    String.format("Sent %d/100 messages", successCount));
            }
        } catch (Exception e) {
            recordResult("Idempotent Producer", Status.FAIL, e.getMessage());
            return;
        }

        // Verify messages arrived by consuming them back
        Properties cProps = new Properties();
        cProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        cProps.put(ConsumerConfig.GROUP_ID_CONFIG, "rc-validator-idempotent-verify-" + UUID.randomUUID());
        cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cProps)) {
            consumer.subscribe(Collections.singleton(topic));
            int consumed = 0;
            int validValues = 0;
            int attempts = 0;
            while (attempts < 15 && consumed < 100) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                for (var record : records) {
                    consumed++;
                    if (record.value() != null && record.value().startsWith("idempotent-value-")) {
                        validValues++;
                    }
                }
                attempts++;
            }
            if (consumed >= 100 && validValues == consumed) {
                recordResult("Idempotent Producer Verify", Status.PASS,
                    String.format("Consumed %d messages, all match expected pattern", consumed));
            } else if (consumed >= 100) {
                recordResult("Idempotent Producer Verify", Status.WARN,
                    String.format("Consumed %d messages, %d matched pattern", consumed, validValues));
            } else {
                recordResult("Idempotent Producer Verify", Status.FAIL,
                    String.format("Expected 100 messages, consumed only %d", consumed));
            }
        } catch (Exception e) {
            recordResult("Idempotent Producer Verify", Status.FAIL, e.getMessage());
        }
    }

    private void testTransactionalProducer() {
        printSection("Transactional Producer Test");

        String topic = topicName("rc-validator-transactional-test");
        String transactionalId = "rc-validator-tx-" + UUID.randomUUID();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Ensure topic exists
        createTopicIfNotExists(topic, 3, (short) 1);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.initTransactions();
            recordResult("Init Transactions", Status.PASS, "Transaction initialized");

            // Test successful transaction
            producer.beginTransaction();
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>(topic, "tx-key-" + i, "committed-value-" + i));
            }
            producer.commitTransaction();
            recordResult("Commit Transaction", Status.PASS, "Transaction committed with 10 messages");

            // Test aborted transaction
            producer.beginTransaction();
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>(topic, "abort-key-" + i, "aborted-value-" + i));
            }
            producer.abortTransaction();
            recordResult("Abort Transaction", Status.PASS, "Transaction aborted successfully");

        } catch (Exception e) {
            recordResult("Transactional Producer", Status.FAIL, e.getMessage());
            return;
        }

        // Verify: read_committed consumer should only see committed messages
        Properties cProps = new Properties();
        cProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        cProps.put(ConsumerConfig.GROUP_ID_CONFIG, "rc-validator-tx-verify-" + UUID.randomUUID());
        cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cProps)) {
            consumer.subscribe(Collections.singleton(topic));
            int committedCount = 0;
            int abortedCount = 0;
            int attempts = 0;
            while (attempts < 10) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                for (var record : records) {
                    if (record.value().startsWith("committed-")) {
                        committedCount++;
                    } else if (record.value().startsWith("aborted-")) {
                        abortedCount++;
                    }
                }
                if (committedCount >= 10) break;
                attempts++;
            }
            if (committedCount == 10 && abortedCount == 0) {
                recordResult("Transaction Isolation", Status.PASS,
                    "read_committed consumer saw 10 committed, 0 aborted messages");
            } else if (abortedCount > 0) {
                recordResult("Transaction Isolation", Status.FAIL,
                    String.format("read_committed consumer saw %d aborted messages (expected 0)", abortedCount));
            } else {
                recordResult("Transaction Isolation", Status.WARN,
                    String.format("Saw %d committed messages (expected 10), 0 aborted", committedCount));
            }
        } catch (Exception e) {
            recordResult("Transaction Isolation", Status.FAIL, e.getMessage());
        }
    }

    // ==================== Consumer Tests ====================

    private void testConsumerWithManualCommit() {
        printSection("Consumer with Manual Commit Test");

        String topic = topicName("rc-validator-idempotent-test");
        String groupId = "rc-validator-manual-commit-" + UUID.randomUUID();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singleton(topic));

            int totalRecords = 0;
            int pollAttempts = 0;
            int maxPollAttempts = 10;

            while (pollAttempts < maxPollAttempts && totalRecords < 50) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                if (!records.isEmpty()) {
                    totalRecords += records.count();
                    consumer.commitSync();
                }
                pollAttempts++;
            }

            if (totalRecords > 0) {
                // Verify committed offsets are non-zero by checking via Admin API
                Properties adminProps = new Properties();
                adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                try (AdminClient admin = AdminClient.create(adminProps)) {
                    var offsets = admin.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata()
                        .get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    long committedTotal = offsets.values().stream()
                        .mapToLong(o -> o.offset())
                        .sum();
                    if (committedTotal > 0 && committedTotal >= totalRecords) {
                        recordResult("Manual Commit Consumer", Status.PASS,
                            String.format("Consumed %d records, committed offset total: %d", totalRecords, committedTotal));
                    } else {
                        recordResult("Manual Commit Consumer", Status.FAIL,
                            String.format("Consumed %d records but committed offset total is %d", totalRecords, committedTotal));
                    }
                }
            } else {
                recordResult("Manual Commit Consumer", Status.WARN, "No records consumed");
            }
        } catch (Exception e) {
            recordResult("Manual Commit Consumer", Status.FAIL, e.getMessage());
        }
    }

    private void testConsumerWithAutoCommit() {
        printSection("Consumer with Auto Commit Test");

        String topic = topicName("rc-validator-idempotent-test");
        String groupId = "rc-validator-auto-commit-" + UUID.randomUUID();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singleton(topic));

            int totalRecords = 0;
            int pollAttempts = 0;

            while (pollAttempts < 10 && totalRecords < 50) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                totalRecords += records.count();
                pollAttempts++;
            }

            if (totalRecords == 0) {
                recordResult("Auto Commit Consumer", Status.WARN, "No records consumed");
                return;
            }

            // Auto-commit fires during poll() after the interval elapses
            Thread.sleep(1500);
            consumer.poll(Duration.ofMillis(500));

            // Verify offsets while consumer is still open
            Properties adminProps = new Properties();
            adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            try (AdminClient admin = AdminClient.create(adminProps)) {
                var offsets = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                long committedTotal = offsets.values().stream()
                    .mapToLong(o -> o.offset())
                    .sum();
                if (committedTotal > 0 && committedTotal >= totalRecords) {
                    recordResult("Auto Commit Consumer", Status.PASS,
                        String.format("Consumed %d records, auto-committed offset total: %d", totalRecords, committedTotal));
                } else {
                    recordResult("Auto Commit Consumer", Status.FAIL,
                        String.format("Consumed %d records but auto-committed offset total is %d", totalRecords, committedTotal));
                }
            }
        } catch (Exception e) {
            recordResult("Auto Commit Consumer", Status.FAIL, e.getMessage());
        }
    }

    private void testConsumerGroupRebalancing() {
        printSection("Consumer Group Rebalancing Test");

        String topic = topicName("rc-validator-rebalance-test");
        String groupId = "rc-validator-rebalance-" + UUID.randomUUID();

        createTopicIfNotExists(topic, 4, (short) 1);

        // Produce some messages first
        produceTestMessages(topic, 100);

        AtomicInteger rebalanceCount = new AtomicInteger(0);

        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                rebalanceCount.incrementAndGet();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                rebalanceCount.incrementAndGet();
            }
        };

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            // Start first consumer
            Future<?> consumer1Future = executor.submit(() -> {
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                    consumer.subscribe(Collections.singleton(topic), listener);
                    for (int i = 0; i < 20; i++) {
                        consumer.poll(Duration.ofSeconds(1));
                    }
                }
            });

            // Wait for first consumer to join
            Thread.sleep(3000);

            // Start second consumer to trigger rebalance
            Future<?> consumer2Future = executor.submit(() -> {
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                    consumer.subscribe(Collections.singleton(topic), listener);
                    for (int i = 0; i < 10; i++) {
                        consumer.poll(Duration.ofSeconds(1));
                    }
                }
            });

            consumer1Future.get(CONSUMER_GROUP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            consumer2Future.get(CONSUMER_GROUP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (rebalanceCount.get() >= 2) {
                recordResult("Consumer Rebalancing", Status.PASS,
                    String.format("Rebalance count: %d", rebalanceCount.get()));
            } else {
                recordResult("Consumer Rebalancing", Status.WARN,
                    String.format("Expected >= 2 rebalances, got: %d", rebalanceCount.get()));
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            recordResult("Consumer Rebalancing", Status.FAIL, e.getMessage());
        } finally {
            executor.shutdownNow();
        }
    }

    private void testPartitionAssignment() {
        printSection("Partition Assignment Test");

        String topic = topicName("rc-validator-partition-test");
        createTopicIfNotExists(topic, 6, (short) 1);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "rc-validator-partition-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // Manual partition assignment
            List<TopicPartition> partitions = Arrays.asList(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(topic, 2)
            );

            consumer.assign(partitions);
            Set<TopicPartition> assigned = consumer.assignment();

            if (assigned.size() == 3) {
                recordResult("Manual Partition Assignment", Status.PASS,
                    "Assigned 3 partitions manually");
            } else {
                recordResult("Manual Partition Assignment", Status.FAIL,
                    "Expected 3 partitions, got: " + assigned.size());
            }

            // Test seekToBeginning and verify position is 0
            consumer.seekToBeginning(partitions);
            long beginPos = consumer.position(partitions.get(0));
            if (beginPos == 0) {
                recordResult("Seek to Beginning", Status.PASS,
                    "Position after seekToBeginning: 0");
            } else {
                recordResult("Seek to Beginning", Status.FAIL,
                    "Expected position 0 after seekToBeginning, got: " + beginPos);
            }

            // Test seekToEnd and verify position >= 0
            consumer.seekToEnd(partitions);
            long endPos = consumer.position(partitions.get(0));
            if (endPos >= 0) {
                recordResult("Seek to End", Status.PASS,
                    "Position after seekToEnd: " + endPos);
            } else {
                recordResult("Seek to End", Status.FAIL,
                    "Invalid position after seekToEnd: " + endPos);
            }

        } catch (Exception e) {
            recordResult("Partition Assignment", Status.FAIL, e.getMessage());
        }
    }

    // ==================== Serialization Tests ====================

    private void testCustomSerializers() {
        printSection("Custom Serializers Test");

        String topic = topicName("rc-validator-serializer-test");
        createTopicIfNotExists(topic, 1, (short) 1);

        // Test with different serializers
        testIntegerSerializer(topic);
        testLongSerializer(topic);
        testByteArraySerializer(topic);
    }

    private void testIntegerSerializer(String topic) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<Integer, String> producer = new KafkaProducer<>(props)) {
            RecordMetadata meta = producer.send(new ProducerRecord<>(topic, 42, "integer-key-test"))
                .get(PRODUCER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (meta.hasOffset() && meta.partition() >= 0) {
                recordResult("Integer Serializer", Status.PASS,
                    String.format("Sent with Integer key, partition=%d offset=%d", meta.partition(), meta.offset()));
            } else {
                recordResult("Integer Serializer", Status.FAIL, "Send returned invalid metadata");
            }
        } catch (Exception e) {
            recordResult("Integer Serializer", Status.FAIL, e.getMessage());
        }
    }

    private void testLongSerializer(String topic) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());

        try (KafkaProducer<Long, Long> producer = new KafkaProducer<>(props)) {
            RecordMetadata meta = producer.send(new ProducerRecord<>(topic, 123456789L, 987654321L))
                .get(PRODUCER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (meta.hasOffset() && meta.partition() >= 0) {
                recordResult("Long Serializer", Status.PASS,
                    String.format("Sent with Long key/value, partition=%d offset=%d", meta.partition(), meta.offset()));
            } else {
                recordResult("Long Serializer", Status.FAIL, "Send returned invalid metadata");
            }
        } catch (Exception e) {
            recordResult("Long Serializer", Status.FAIL, e.getMessage());
        }
    }

    private void testByteArraySerializer(String topic) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            byte[] key = "byte-key".getBytes();
            byte[] value = new byte[1024]; // 1KB of data
            new Random().nextBytes(value);

            RecordMetadata meta = producer.send(new ProducerRecord<>(topic, key, value))
                .get(PRODUCER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (meta.hasOffset() && meta.serializedValueSize() == 1024) {
                recordResult("ByteArray Serializer", Status.PASS,
                    String.format("Sent 1KB byte[] payload, serialized size verified (%d bytes)", meta.serializedValueSize()));
            } else if (meta.hasOffset()) {
                recordResult("ByteArray Serializer", Status.FAIL,
                    String.format("Expected serialized size 1024, got %d", meta.serializedValueSize()));
            } else {
                recordResult("ByteArray Serializer", Status.FAIL, "Send returned invalid metadata");
            }
        } catch (Exception e) {
            recordResult("ByteArray Serializer", Status.FAIL, e.getMessage());
        }
    }

    // ==================== Kafka Streams Tests ====================

    private void testKafkaStreamsTopology() {
        printSection("Kafka Streams Topology Test");

        String inputTopic = topicName("rc-validator-streams-input");
        String outputTopic = topicName("rc-validator-streams-output");

        createTopicIfNotExists(inputTopic, 3, (short) 1);
        createTopicIfNotExists(outputTopic, 3, (short) 1);

        // Produce test data
        produceTestMessages(inputTopic, 50);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rc-validator-streams-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        StreamsBuilder builder = new StreamsBuilder();

        // Simple topology: read from input, transform, write to output
        builder.<String, String>stream(inputTopic)
            .filter((key, value) -> value != null)
            .mapValues(value -> "processed-" + value)
            .to(outputTopic);

        Topology topology = builder.build();

        try {
            // Verify topology can be built
            String topologyDescription = topology.describe().toString();
            if (topologyDescription.contains(inputTopic) && topologyDescription.contains(outputTopic)) {
                recordResult("Streams Topology Build", Status.PASS, "Topology built successfully");
            } else {
                recordResult("Streams Topology Build", Status.FAIL, "Topology missing expected topics");
            }
        } catch (Exception e) {
            recordResult("Streams Topology Build", Status.FAIL, e.getMessage());
            return;
        }

        // Run the streams application briefly
        try (KafkaStreams streams = new KafkaStreams(topology, props)) {
            CountDownLatch latch = new CountDownLatch(1);

            streams.setStateListener((newState, oldState) -> {
                if (newState == KafkaStreams.State.RUNNING) {
                    latch.countDown();
                }
            });

            streams.start();

            if (latch.await(CONSUMER_GROUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                recordResult("Streams Application Start", Status.PASS, "Streams app reached RUNNING state");

                // Let it process
                Thread.sleep(5000);

                // Check state
                if (streams.state() == KafkaStreams.State.RUNNING) {
                    recordResult("Streams Application Running", Status.PASS, "Streams app still running");
                } else {
                    recordResult("Streams Application Running", Status.WARN,
                        "Streams state: " + streams.state());
                }
            } else {
                recordResult("Streams Application Start", Status.FAIL,
                    "Streams app did not reach RUNNING state in time");
            }

            streams.close(Duration.ofSeconds(10));
        } catch (Exception e) {
            recordResult("Streams Application", Status.FAIL, e.getMessage());
            return;
        }

        // Verify output topic has processed records with the expected prefix
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "rc-validator-streams-verify-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singleton(outputTopic));
            int outputRecords = 0;
            int correctlyProcessed = 0;
            int attempts = 0;
            while (attempts < 10 && outputRecords < 10) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                for (var record : records) {
                    outputRecords++;
                    if (record.value() != null && record.value().startsWith("processed-")) {
                        correctlyProcessed++;
                    }
                }
                attempts++;
            }
            if (correctlyProcessed > 0) {
                recordResult("Streams Output Verification", Status.PASS,
                    String.format("Found %d processed records in output topic", correctlyProcessed));
            } else if (outputRecords > 0) {
                recordResult("Streams Output Verification", Status.FAIL,
                    String.format("Found %d records but none with 'processed-' prefix", outputRecords));
            } else {
                recordResult("Streams Output Verification", Status.WARN,
                    "No records found in output topic (processing may need more time)");
            }
        } catch (Exception e) {
            recordResult("Streams Output Verification", Status.FAIL, e.getMessage());
        }
    }

    // ==================== Metrics Tests ====================

    private void testMetricsAccess() {
        printSection("Metrics Access Test");

        String topic = topicName("rc-validator-metrics-test");
        createTopicIfNotExists(topic, 1, (short) 1);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, "metrics-test", "value"))
                .get(PRODUCER_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> metrics =
                producer.metrics();

            if (metrics != null && !metrics.isEmpty()) {
                recordResult("Producer Metrics", Status.PASS,
                    String.format("Found %d metrics", metrics.size()));

                boolean foundRecordSendTotal = metrics.keySet().stream()
                    .anyMatch(m -> "record-send-total".equals(m.name()));

                if (foundRecordSendTotal) {
                    recordResult("Record Send Metrics", Status.PASS, "Found record-send-total metric");
                } else {
                    recordResult("Record Send Metrics", Status.WARN, "record-send-total metric not found");
                }
            } else {
                recordResult("Producer Metrics", Status.FAIL, "No metrics available");
            }
        } catch (Exception e) {
            recordResult("Metrics Access", Status.FAIL, e.getMessage());
        }
    }

    // ==================== Cleanup Tests ====================

    private void testTopicDeletion() {
        printSection("Topic Deletion Test");

        String testTopic = "rc-validator-delete-test-" + UUID.randomUUID();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(props)) {
            // Create topic
            NewTopic newTopic = new NewTopic(testTopic, 1, (short) 1);
            admin.createTopics(Collections.singleton(newTopic)).all().get(CREATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // Verify it exists
            Set<String> topics = admin.listTopics().names().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!topics.contains(testTopic)) {
                recordResult("Topic Deletion", Status.FAIL, "Topic was not created");
                return;
            }

            // Delete topic
            admin.deleteTopics(Collections.singleton(testTopic)).all().get(CREATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // Verify deletion (may take a moment)
            Thread.sleep(2000);
            topics = admin.listTopics().names().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (!topics.contains(testTopic)) {
                recordResult("Topic Deletion", Status.PASS, "Topic deleted successfully");
            } else {
                recordResult("Topic Deletion", Status.WARN, "Topic still exists after deletion request");
            }
        } catch (Exception e) {
            recordResult("Topic Deletion", Status.FAIL, e.getMessage());
        }
    }

    // ==================== Helper Methods ====================

    private void cleanupTopics() {
        if (createdTopics.isEmpty()) return;

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(props)) {
            admin.deleteTopics(createdTopics).all().get(CREATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            System.out.println("\nCleaned up " + createdTopics.size() + " test topics");
        } catch (Exception e) {
            System.err.println("Warning: topic cleanup failed: " + e.getMessage());
        }
    }

    private void createTopicIfNotExists(String topic, int partitions, short replicationFactor) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic(topic, partitions, replicationFactor);
            admin.createTopics(Collections.singleton(newTopic)).all().get(CREATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            createdTopics.add(topic);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                createdTopics.add(topic);
            } else {
                throw new RuntimeException("Failed to create topic " + topic, e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted creating topic " + topic, e);
        } catch (TimeoutException e) {
            throw new RuntimeException("Timeout creating topic " + topic, e);
        }
    }

    private void produceTestMessages(String topic, int count) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                futures.add(producer.send(new ProducerRecord<>(topic, "key-" + i, "value-" + i)));
            }
            producer.flush();
            for (Future<RecordMetadata> f : futures) {
                f.get(PRODUCER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to produce test messages to " + topic, e);
        }
    }

    private void recordResult(String test, Status status, String message) {
        results.add(new TestResult(test, status, message));

        String statusColor = switch (status) {
            case PASS -> ANSI_GREEN;
            case FAIL -> ANSI_RED;
            case WARN -> ANSI_YELLOW;
            case SKIP -> ANSI_BLUE;
        };

        System.out.println(statusColor + "[" + status + "]" + ANSI_RESET + " " + test + ": " + message);
    }

    private void printSection(String title) {
        System.out.println("\n" + ANSI_BLUE + "--- " + title + " ---" + ANSI_RESET);
    }

    private void printSummary() {
        System.out.println("\n" + ANSI_BLUE + "=============================================" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "Validation Summary" + ANSI_RESET);
        System.out.println(ANSI_BLUE + "=============================================" + ANSI_RESET + "\n");

        long passed = results.stream().filter(r -> r.status == Status.PASS).count();
        long failed = results.stream().filter(r -> r.status == Status.FAIL).count();
        long warned = results.stream().filter(r -> r.status == Status.WARN).count();
        long skipped = results.stream().filter(r -> r.status == Status.SKIP).count();

        System.out.println("Total tests: " + results.size());
        System.out.println(ANSI_GREEN + "Passed: " + passed + ANSI_RESET);
        System.out.println(ANSI_RED + "Failed: " + failed + ANSI_RESET);
        System.out.println(ANSI_BLUE + "Skipped: " + skipped + ANSI_RESET);
        System.out.println(ANSI_YELLOW + "Warnings: " + warned + ANSI_RESET);

        if (failed > 0) {
            System.out.println("\n" + ANSI_RED + "Failed tests:" + ANSI_RESET);
            results.stream()
                .filter(r -> r.status == Status.FAIL)
                .forEach(r -> System.out.println("  - " + r.test + ": " + r.message));
        }

        System.out.println();
    }

    private enum Status {
        PASS, FAIL, WARN, SKIP
    }

    private record TestResult(String test, Status status, String message) {}
}
