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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for AddFieldConfig.
 */
public class AddFieldConfigTest {

  private static final String MOCK_STAGE = "mockStage";
  private static final AddFieldConfig VALID_CONFIG = new AddFieldConfig(
    "test",
    "s_value",
    false
  );
  private static final Schema INPUT_SCHEMA = Schema.recordOf("input-record",
                                                             Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                                             Schema.Field.of("input", Schema.of(Schema.Type.STRING)));

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector, INPUT_SCHEMA);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateFieldNameExistsInInputSchema() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    AddFieldConfig config = AddFieldConfig.builder(VALID_CONFIG)
      .setFieldName("id")
      .build();

    List<List<String>> paramName = Collections.singletonList(
      Collections.singletonList(AddFieldConfig.FIELD_NAME));

    config.validate(failureCollector, INPUT_SCHEMA);
    ValidationAssertions.assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testValidateFieldValueEmptyAsUUIDFalse() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    AddFieldConfig config = AddFieldConfig.builder(VALID_CONFIG)
      .setFieldValue(null)
      .build();

    List<List<String>> paramName = Collections.singletonList(
      Arrays.asList(AddFieldConfig.FIELD_VALUE, AddFieldConfig.AS_UUID));

    config.validate(failureCollector, INPUT_SCHEMA);
    ValidationAssertions.assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testValidateFieldValueNotEmptyAsUUIDTrue() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    AddFieldConfig config = AddFieldConfig.builder(VALID_CONFIG)
      .setAsUUID(true)
      .build();

    List<List<String>> paramName = Collections.singletonList(
      Arrays.asList(AddFieldConfig.FIELD_VALUE, AddFieldConfig.AS_UUID));

    config.validate(failureCollector, INPUT_SCHEMA);
    ValidationAssertions.assertValidationFailed(failureCollector, paramName);
  }
}
