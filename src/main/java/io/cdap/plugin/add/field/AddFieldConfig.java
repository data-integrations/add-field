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
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import javax.annotation.Nullable;

/**
 * Config properties for the plugin.
 */
public class AddFieldConfig extends PluginConfig {
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

  public AddFieldConfig() {
  }

  public AddFieldConfig(String fieldName, @Nullable String fieldValue, @Nullable Boolean asUUID) {
    this.fieldName = fieldName;
    this.fieldValue = fieldValue;
    this.asUUID = asUUID;
  }

  private AddFieldConfig(Builder builder) {
    fieldName = builder.fieldName;
    fieldValue = builder.fieldValue;
    asUUID = builder.asUUID;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(AddFieldConfig copy) {
    return builder()
      .setFieldName(copy.fieldName)
      .setFieldValue(copy.fieldValue)
      .setAsUUID(copy.asUUID);
  }

  public String getFieldName() {
    return fieldName;
  }

  @Nullable
  public String getFieldValue() {
    return fieldValue;
  }

  public Boolean getAsUUID() {
    return asUUID == null ? false : asUUID;
  }

  public void validate(FailureCollector failureCollector, @Nullable Schema inputSchema) {
    if (!containsMacro(FIELD_VALUE)) {
      if (fieldValue == null && !getAsUUID()) {
        failureCollector.addFailure("Must specify a field value or set 'Generate UUID as Value' to true.",
                                    null)
          .withConfigProperty(FIELD_VALUE)
          .withConfigProperty(AS_UUID);
      }

      if (fieldValue != null && getAsUUID()) {
        failureCollector.addFailure("Must not specify a field value or set 'Generate UUID as Value' to false.",
                                    null)
          .withConfigProperty(FIELD_VALUE)
          .withConfigProperty(AS_UUID);
      }
    }

    if (inputSchema != null) {
      for (Schema.Field field : inputSchema.getFields()) {
        if (field.getName().equals(fieldName)) {
          failureCollector.addFailure(String.format("Field '%s' already exists in the input schema.", fieldName),
                                      "Provide field that not present the input schema.")
            .withConfigProperty(FIELD_NAME)
            .withInputSchemaField(fieldName);
          break;
        }
      }
    }
  }

  /**
   * Builder for AddFieldConfig
   */
  public static final class Builder {
    private String fieldName;
    private String fieldValue;
    private Boolean asUUID;

    private Builder() {
    }

    public Builder setFieldName(String fieldName) {
      this.fieldName = fieldName;
      return this;
    }

    public Builder setFieldValue(String fieldValue) {
      this.fieldValue = fieldValue;
      return this;
    }

    public Builder setAsUUID(Boolean asUUID) {
      this.asUUID = asUUID;
      return this;
    }

    public AddFieldConfig build() {
      return new AddFieldConfig(fieldName, fieldValue, asUUID);
    }
  }
}
