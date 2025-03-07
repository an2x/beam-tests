package pipelines;

import static autovalue.shaded.com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.openjdk.nashorn.api.scripting.ScriptObjectMirror;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UdfTransform extends PTransform<PCollection<String>, PCollection<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(UdfTransform.class);

  private final ValueProvider<String> jsPathProvider;
  private final ValueProvider<String> jsFunctionNameProvider;

  public UdfTransform(ValueProvider<String> jsPathProvider, ValueProvider<String> jsFunctionNameProvider) {
    this.jsPathProvider = jsPathProvider;
    this.jsFunctionNameProvider = jsFunctionNameProvider;
  }

  @Override
  public PCollection<String> expand(PCollection<String> strings) {
    return strings.apply(
        ParDo.of(
            new DoFn<String, String>() {
              private transient String jsFunctionName;
              private transient Invocable invocable;

              @Setup
              public void setup() throws IOException, ScriptException {
                String jsPath = jsPathProvider.get();
                String jsFunctionName = jsFunctionNameProvider.get();

                if (jsPath == null || jsPath.isEmpty()) {
                  LOG.warn("jsPath parameter not set, skipping UDF step");
                  return;
                }
                if (jsFunctionName == null || jsFunctionName.isEmpty()) {
                  LOG.warn("jsFunctionName parameter not set, skipping UDF step");
                  return;
                }

                this.jsFunctionName = jsFunctionName;

                Collection<String> scripts = getScripts(jsPath);
                ScriptEngine engine = getJavaScriptEngine();
                LOG.info("Using JavaScript engine: {}, version: {}",
                    engine.getFactory().getEngineName(), engine.getFactory().getEngineVersion());
                for (String script : scripts) {
                  engine.eval(script);
                }
                this.invocable = (Invocable) engine;
              }

              @ProcessElement
              public void processElement(ProcessContext c) throws IOException, NoSuchMethodException, ScriptException {
                if (this.jsFunctionName == null) { // means UDF params were not enabled
                  c.output(c.element());
                  return;
                }

                String element = invoke(c.element());
                if (!Strings.isNullOrEmpty(element)) {
                  c.output(element);
                }
              }

              private String invoke(String data) {
                if (invocable == null) {
                  throw new RuntimeException("No UDF was loaded");
                }

                Object result;

                try {
                  result = invocable.invokeFunction(jsFunctionName, data);
                } catch (Exception e) {
                  throw new RuntimeException("Error invoking UDF on data: " + data);
                }

                if (result == null || ScriptObjectMirror.isUndefined(result)) {
                  return null;
                } else if (result instanceof String) {
                  return (String) result;
                } else {
                  String className = result.getClass().getName();
                  throw new RuntimeException(
                      "UDF Function did not return a String. Instead got: " + className);
                }
              }
            }));
  }

  private static ScriptEngine getJavaScriptEngine() {
    NashornScriptEngineFactory nashornFactory = new NashornScriptEngineFactory();
    ScriptEngine engine = nashornFactory.getScriptEngine("--language=es6");

    if (engine != null) {
      return engine;
    }

    List<String> availableEngines = new ArrayList<>();
    ScriptEngineManager manager = new ScriptEngineManager();
    for (ScriptEngineFactory factory : manager.getEngineFactories()) {
      availableEngines.add(
          factory.getEngineName() + " (" + factory.getEngineVersion() + ") - " + factory.getNames());
    }

    throw new RuntimeException(
        String.format("JavaScript engine not available. Found engines: %s.", availableEngines));
  }

  private static Collection<String> getScripts(String path) throws IOException {
    MatchResult result = FileSystems.match(path);
    checkArgument(
        result.status() == Status.OK && !result.metadata().isEmpty(),
        "Failed to match any files with the pattern: " + path);

    List<String> scripts =
        result.metadata().stream()
            .filter(metadata -> metadata.resourceId().getFilename().endsWith(".js"))
            .map(Metadata::resourceId)
            .map(
                resourceId -> {
                  try (Reader reader =
                      Channels.newReader(
                          FileSystems.open(resourceId), StandardCharsets.UTF_8.name())) {
                    return CharStreams.toString(reader);
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                })
            .collect(Collectors.toList());

    return scripts;
  }
}
