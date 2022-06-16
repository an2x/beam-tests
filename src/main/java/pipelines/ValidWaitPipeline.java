package pipelines;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidWaitPipeline {

  private final static Logger LOG = LoggerFactory.getLogger(ValidWaitPipeline.class);

  public interface Options extends DataflowPipelineOptions {

  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<Integer> writtenNodes1 = pipeline
        .apply("Read Nodes 1", Create.of("node11", "node12", "node13"))
        .apply("Write Nodes 1", new WriteWrapper(100));

    PCollection<Integer> writtenNodes2 = pipeline
        .apply("Read Nodes 2", Create.of("node21", "node22", "node23"))
        .apply("Write Nodes 2", new WriteWrapper(1000));

    PCollection<Integer> allNodes =
        PCollectionList.of(writtenNodes1).and(writtenNodes2)
            .apply(Flatten.pCollections());

    PCollection<Integer> writtenEdges = pipeline
        .apply("Read Edges", Create.of("edge1", "edge2", "edge3"))
        .apply("Wait for Nodes", Wait.on(allNodes))
        .apply("Write Edges", new WriteWrapper(100));

    pipeline.run();
  }

  private static class WriteWrapper extends
      PTransform<PCollection<String>, PCollection<Integer>> {

    private final int delay;

    public WriteWrapper(int delay) {
      this.delay = delay;
    }

    @Override
    public PCollection<Integer> expand(PCollection<String> input) {
      PCollection<Integer> writeOutput = input.apply(new FakeNeo4jWriteUnwind(delay));
      return writeOutput;
    }
  }

  private static class FakeNeo4jWriteUnwind extends
      PTransform<PCollection<String>, PCollection<Integer>> {

    private final int delay;

    public FakeNeo4jWriteUnwind(int delay) {
      this.delay = delay;
    }

    @Override
    public PCollection<Integer> expand(PCollection<String> input) {
      return input.apply(ParDo.of(new DoFn<String, Integer>() {
        @ProcessElement
        public void processElement(@Element String element) throws Exception {
          Thread.sleep(delay);
          LOG.info("Element written: {}", element);
          // This doesn't produce any elements so the output collection will be empty.
        }
      }));
    }
  }
}
