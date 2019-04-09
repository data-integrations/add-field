/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.plugin;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.ws.rs.Path;

/**
 * Transform that can transforms specific fields to lowercase or uppercase.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(AddField.NAME)
@Description("Adds a new field to each input record whose value can either be a new UUID, or a configured value.")
public class AddField extends Transform<StructuredRecord, StructuredRecord> {
  public static final String NAME = "AddField";
  private final Conf config;

  /**
   * Config properties for the plugin.
   */
  public static class Conf extends PluginConfig {
    public static final String FIELD_NAME = "fieldName";
    public static final String FIELD_VALUE = "fieldValue";
    public static final String AS_UUID = "asUUID";

    @Name(FIELD_NAME)
    @Description("The name of the field to add. Must not already exist as an input field. The field type will be " +
      "a nullable string.")
    private String fieldName;

    @Macro
    @Nullable
    @Name(FIELD_VALUE)
    @Description("The value to set for the new field. If this is not specified, 'asUUID' must be set to true.")
    private String fieldValue;

    @Nullable
    @Name(AS_UUID)
    @Description("Generate a new UUID for the new field. If this is not true, 'fieldValue' must be specified.")
    private Boolean asUUID;

    public Conf() {
      asUUID = false;
    }

    private void validate(@Nullable Schema inputSchema) {
      if (fieldValue == null && !containsMacro(FIELD_VALUE) && !asUUID) {
        throw new IllegalArgumentException("Must specify a field value or set 'asUUID' to true.");
      }

      if (inputSchema != null) {
        for (Schema.Field field : inputSchema.getFields()) {
          if (field.getName().equals(fieldName)) {
            throw new IllegalArgumentException(
              String.format("Field '%s' already exists in the input schema.", fieldName));
          }
        }
      }
    }
  }

  /**
   * Endpoint request for output schema.
   */
  public static class GetSchemaRequest extends Conf {
    private Schema inputSchema;
  }

  public AddField(Conf config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    config.validate(inputSchema);
    if (inputSchema != null) {
      stageConfigurer.setOutputSchema(getOutputSchema(inputSchema, config));
    }
  }

  // transform is called once for each record that goes into this stage
  @Override
  public void transform(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
    Schema outputSchema = getOutputSchema(record.getSchema(), config);
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field inputField : record.getSchema().getFields()) {
      String inputFieldName = inputField.getName();
      // this can only happen when the input schema is not constant and known at configure time
      if (inputFieldName.equals(config.fieldName)) {
        emitter.emitError(new InvalidEntry<>(400, String.format("field '%s' already exists in input", config.fieldName),
                                             record));
        return;
      }
      builder.set(inputFieldName, record.get(inputFieldName));
    }
    String newFieldVal = config.asUUID ? UUID.randomUUID().toString() : config.fieldValue;
    builder.set(config.fieldName, newFieldVal);
    emitter.emit(builder.build());
  }

  @Path("outputSchema")
  public Schema getOutputSchema(GetSchemaRequest request) {
    return getOutputSchema(request.inputSchema, request);
  }

  private Schema getOutputSchema(Schema inputSchema, Conf config) {
    List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields().size() + 1);
    fields.addAll(inputSchema.getFields());
    fields.add(Schema.Field.of(config.fieldName, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    return Schema.recordOf(inputSchema.getRecordName() + ".added", fields);
  }

}
