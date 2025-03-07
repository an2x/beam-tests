package pipelines;

import java.time.Duration;
import java.time.Instant;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubUdfPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubUdfPipeline.class);

  public interface Options extends DataflowPipelineOptions {
    ValueProvider<String> getSourceSubscription();

    void setSourceSubscription(ValueProvider<String> value);

    ValueProvider<String> getJsPath();

    void setJsPath(ValueProvider<String> value);

    ValueProvider<String> getJsFunctionName();

    void setJsFunctionName(ValueProvider<String> value);

    @Default.Integer(0)
    ValueProvider<Integer> getLogIntervalSeconds();

    void setLogIntervalSeconds(ValueProvider<Integer> value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Read from Pub/Sub", PubsubIO.readStrings().fromSubscription(options.getSourceSubscription()))
        .apply("Apply UDF", new UdfTransform(options.getJsPath(), options.getJsFunctionName()))
        .apply("Log Messages", ParDo.of(new PeriodicLoggingFn(options.getLogIntervalSeconds())));

    pipeline.run();
  }

  public static class PeriodicLoggingFn extends DoFn<String, Void> {

    private final ValueProvider<Integer> logIntervalSeconds;
    private transient Instant lastLogTime;
    private transient int interval = 0;

    public PeriodicLoggingFn(ValueProvider<Integer> logIntervalSeconds) {
      this.logIntervalSeconds = logIntervalSeconds;
    }

    @Setup
    public void setup() {
      this.interval = logIntervalSeconds.get();
      LOG.info("Will log messages every {} seconds", this.interval);
    }

    @ProcessElement
    public void processElement(@Element String element) {
      if (interval > 0) {
        Instant currentTime = Instant.now();
        if (lastLogTime == null || Duration.between(lastLogTime, currentTime).getSeconds() >= interval) {
          LOG.info("MESSAGE: {}", element);
          lastLogTime = currentTime;
        }
      }
    }
  }
}
