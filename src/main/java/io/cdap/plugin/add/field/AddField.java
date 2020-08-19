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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.ws.rs.Path;

/**
 * Transform that can transforms specific fields to lowercase or uppercase.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(AddField.NAME)
@Description("Adds a new field to each input record whose value can either be a new UUID, or a configured value.")
public class AddField extends Transform<StructuredRecord, StructuredRecord> {
  public static final String NAME = "AddField";
  private final AddFieldConfig config;

  /**
   * Endpoint request for output schema.
   */
  public static class GetSchemaRequest extends AddFieldConfig {
    private Schema inputSchema;
  }

  public AddField(AddFieldConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    FailureCollector failureCollector = stageConfigurer.getFailureCollector();
    config.validate(failureCollector, inputSchema);

    if (inputSchema != null) {
      stageConfigurer.setOutputSchema(getOutputSchema(inputSchema, config));
    }
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    FailureCollector failureCollector = context.getFailureCollector();
    Schema inputSchema = context.getInputSchema();

    config.validate(failureCollector, inputSchema);
    failureCollector.getOrThrowException();
  }

  // transform is called once for each record that goes into this stage
  @Override
  public void transform(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
    Schema outputSchema = getOutputSchema(record.getSchema(), config);
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field inputField : record.getSchema().getFields()) {
      String inputFieldName = inputField.getName();
      // this can only happen when the input schema is not constant and known at configure time
      if (inputFieldName.equals(config.getFieldName())) {
        emitter.emitError(new InvalidEntry<>(400, String.format("field '%s' already exists in input",
                                                                config.getFieldName()), record));
        return;
      }
      builder.set(inputFieldName, record.get(inputFieldName));
    }
    String newFieldVal = config.getAsUUID() ? UUID.randomUUID().toString() : config.getFieldValue();
    builder.set(config.getFieldName(), newFieldVal);
    emitter.emit(builder.build());
  }

  @Path("outputSchema")
  public Schema getOutputSchema(GetSchemaRequest request) {
    return getOutputSchema(request.inputSchema, request);
  }

  private Schema getOutputSchema(Schema inputSchema, AddFieldConfig config) {
    List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields().size() + 1);
    fields.addAll(inputSchema.getFields());
    fields.add(Schema.Field.of(config.getFieldName(), Schema.of(Schema.Type.STRING)));
    return Schema.recordOf(inputSchema.getRecordName() + ".added", fields);
  }

}
