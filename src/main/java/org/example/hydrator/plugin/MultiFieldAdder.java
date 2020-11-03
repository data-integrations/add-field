package org.example.hydrator.plugin;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.ws.rs.Path;

/**
 * This class <code>MultiFieldAdder</code> is a transform that would add additional
 * field names and constant values to the output schema.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(MultiFieldAdder.NAME)
@Description("Adds multiple fields to the output record.")
public class MultiFieldAdder extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(MultiFieldAdder.class);
  public static final String NAME = "MultiFieldAdder";
  private final Conf config;
  private Map<String, String> fieldMaps = new TreeMap<>();

  public static class Conf extends PluginConfig {
    @Name("fieldValue")
    @Description("Specify a field value pair that needs to added to output.")
    private String fieldValue;

    Map<String, String> getFieldValue() throws IllegalArgumentException {
      Map<String, String> values = new TreeMap<>();
      if (fieldValue == null || fieldValue.trim().isEmpty()) {
        return values;
      }

      String[] fvPairs = fieldValue.split(",");
      for (String fvPair : fvPairs) {
        String[] fieldAndValue = fvPair.split(":");
        if (fieldAndValue.length != 2) {
          continue;
        }
        String fieldName = fieldAndValue[0];
        String fieldValue = fieldAndValue[1];
        if (values.containsKey(fieldName)) {
          throw new IllegalArgumentException(
            String.format("Field '%s' is specified multiple times.")
          );
        }
        values.put(fieldName, fieldValue);
      }
      return values;
    }
  }

  public static class GetSchemaRequest extends Conf {
    private Schema inputSchema;
  }

  public MultiFieldAdder(Conf config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    StageConfigurer stageConfigurer = configurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    config.getFieldValue();
    if (inputSchema != null) {
      stageConfigurer.setOutputSchema(getOutputSchema(inputSchema, config));
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    fieldMaps = config.getFieldValue();
  }

  @Override
  public void transform(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
    Schema outputSchema = getOutputSchema(record.getSchema(), config);
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field inputField : record.getSchema().getFields()) {
      String inputFieldName = inputField.getName();
      if (fieldMaps.containsKey(inputFieldName)) {
        emitter.emitError(new InvalidEntry<>(400, String.format("Field '%s' already exists in input", inputFieldName),
                                             record));
        return;
      }
      builder.set(inputFieldName, record.get(inputFieldName));
    }
    for(Map.Entry<String, String> fieldValue : fieldMaps.entrySet()) {
      builder.set(fieldValue.getKey(), fieldValue.getValue());
    }
    emitter.emit(builder.build());
  }

  @Path("outputSchema")
  public Schema getOutputSchema(GetSchemaRequest request) {
    return getOutputSchema(request.inputSchema, request);
  }

  private Schema getOutputSchema(Schema schema, Conf config) {
    if (schema == null) {
      throw new IllegalArgumentException("No node is connected. Please connect a node to generate the schema.");
    }
    List<Schema.Field> fields = new ArrayList<>(schema.getFields().size() + 1);
    fields.addAll(schema.getFields());
    Map<String, String> values = config.getFieldValue();
    for (Map.Entry<String, String> value : values.entrySet()) {
      fields.add(Schema.Field.of(value.getKey(), Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    }
    return Schema.recordOf(schema.getRecordName() + ".added", fields);
  }
}
