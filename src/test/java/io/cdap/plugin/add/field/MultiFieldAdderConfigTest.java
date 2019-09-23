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

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Unit tests for MultiFieldAdderConfig.
 */
public class MultiFieldAdderConfigTest {

  private static final String MOCK_STAGE = "mockStage";
  private static final MultiFieldAdderConfig VALID_CONFIG = new MultiFieldAdderConfig(
    "test_1:s_value_1,test_2:s_value_2,test_3:s_value_3"
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateDuplicateFieldName() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    MultiFieldAdderConfig config = MultiFieldAdderConfig.builder(VALID_CONFIG)
      .setFieldValue("test_1:s_value_1,test_1:s_value_2")
      .build();
    List<List<String>> paramName = Collections.singletonList(
      Collections.singletonList(MultiFieldAdderConfig.FIELD_VALUE));

    config.validate(failureCollector);
    ValidationAssertions.assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testValidateFieldNameWithoutValue() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    MultiFieldAdderConfig config = MultiFieldAdderConfig.builder(VALID_CONFIG)
      .setFieldValue("test_1:s_value_1,test_2,test_3:s_value_3")
      .build();

    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateDuplicateFieldNameWithoutValue() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    MultiFieldAdderConfig config = MultiFieldAdderConfig.builder(VALID_CONFIG)
      .setFieldValue("test_1:s_value_1,test_2,test_2:s_value_2")
      .build();

    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }
}
