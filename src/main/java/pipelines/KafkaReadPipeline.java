package pipelines;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaReadPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaReadPipeline.class);

  public interface Options extends DataflowPipelineOptions {
    @Validation.Required
    String getBootstrapServer();
    void setBootstrapServer(String value);

    @Validation.Required
    String getTopic();
    void setTopic(String value);

    String getConsumerGroupId();
    void setConsumerGroupId(String value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    String bootstrapServer = options.getBootstrapServer();
    String topic = options.getTopic();
    int partitionCount = 1;

    Preconditions.checkNotNull(bootstrapServer);
    Preconditions.checkNotNull(topic);

    Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    consumerConfig.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
    consumerConfig.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
        "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler");
    consumerConfig.put(SaslConfigs.SASL_JAAS_CONFIG,
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
    if (options.getConsumerGroupId() != null) {
      consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, options.getConsumerGroupId());
    }

    List<TopicPartition> topicPartitions = new ArrayList<>();
    for (int i = 0; i < partitionCount; i++) {
      topicPartitions.add(new TopicPartition(topic, i));
    }

    pipeline
        .apply("Read from Kafka",
            KafkaIO.<String, String>read()
                .withBootstrapServers(bootstrapServer)
                .withTopicPartitions(topicPartitions)
                .withKeyDeserializerAndCoder(StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
                .withValueDeserializerAndCoder(StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
                .withConsumerConfigUpdates(ImmutableMap.copyOf(consumerConfig))
                //.withReadCommitted()
                .commitOffsetsInFinalize())
        .apply("Log Records",
            ParDo.of(new DoFn<KafkaRecord<String, String>, Void>() {
              @ProcessElement
              public void processElement(@Element KafkaRecord<String, String> e) {
                LOG.info(
                    "Received element: topic = {}, partition = {}, offset = {}, timestamp = {}, key = {}, value = {}",
                    e.getTopic(), e.getPartition(), e.getOffset(), e.getTimestamp(), e.getKV().getKey(),
                    e.getKV().getValue());
              }
            }));

    pipeline.run();
  }
}
