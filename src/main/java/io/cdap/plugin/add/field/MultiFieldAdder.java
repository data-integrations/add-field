/*
 * Copyright © 2016-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.add.field;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
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
  private final MultiFieldAdderConfig config;
  private Map<String, String> fieldMaps = new TreeMap<>();

  public static class GetSchemaRequest extends MultiFieldAdderConfig {
    private Schema inputSchema;
  }

  public MultiFieldAdder(MultiFieldAdderConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    StageConfigurer stageConfigurer = configurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    FailureCollector failureCollector = stageConfigurer.getFailureCollector();
    config.validate(failureCollector);

    if (inputSchema != null) {
      stageConfigurer.setOutputSchema(getOutputSchema(inputSchema, config));
    }
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    FailureCollector failureCollector = context.getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();
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

  private Schema getOutputSchema(Schema schema, MultiFieldAdderConfig config) {
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
