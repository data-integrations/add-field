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

import io.cdap.cdap.api.macro.Macros;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.FieldSetter;

import java.util.*;

/**
 * Unit tests for MultiFieldAdderConfig.
 */
public class MultiFieldAdderConfigTest {

  private static final String MOCK_STAGE = "mockStage";
  private static final MultiFieldAdderConfig VALID_CONFIG = new MultiFieldAdderConfig(
    "test_1:s_value_1,test_2:s_value_2,test_3:s_value_3"
  );
  private static final String ALL_KEYS_MACRO = "${test_1}:value_1,${test_2}:value_2,${test_3}:value_3";
  private static final String NONE_KEY_MACRO = "test_1:${value_1},test_2:value_2,test_3:${value_3}";
  private static final String NESTED_MACRO_KEY = "${test_1${nested_1}},value_1,${test_2${nested_2}}:value_2," +
          "${test_3${nested_3}}:value_3";
  private static final String NESTED_MACRO_VALUE = "test_1:${value_1${nested_1}},test_2:value_2," +
          "test_3:${value_3${nested_3}}";
  private static final String PARTIAL_MACRO_KEYS = "test_1:${value_1},test_2:value_2,test_3:${value_3}" +
          ",${test_4}:value_4,${test_5}:value_5";
  private static final String SECURE_MACRO = "test_1:${value_1},test_2:value_2,test_3:${value_3}," +
          "${secure(test_4)}:${secure(value_4)}";
  private static final String WHOLE_MACRO = "${value}";

  private static final Map<String,String> MACRO_EXPECTED_VALUES = new HashMap<>();
  static {
    MACRO_EXPECTED_VALUES.put("test_1", null);
    MACRO_EXPECTED_VALUES.put("test_2", "value_2");
    MACRO_EXPECTED_VALUES.put("test_3", null);
  }

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

  @Test
  public void testAllKeysMacro() throws NoSuchFieldException {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    MultiFieldAdderConfig config = getMacroMultiFieldAdderConfig(ALL_KEYS_MACRO);

    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertEquals(ALL_KEYS_MACRO, config.getMacroFieldValue());
    Assert.assertTrue(config.getFieldValue().isEmpty());
  }

  @Test
  public void testNoneKeysMacro() throws NoSuchFieldException {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    MultiFieldAdderConfig config = getMacroMultiFieldAdderConfig(NONE_KEY_MACRO);

    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertEquals(NONE_KEY_MACRO, config.getMacroFieldValue());
    Assert.assertEquals(MACRO_EXPECTED_VALUES, config.getFieldValue());
  }

  @Test
  public void testNestedMacroKey() throws NoSuchFieldException {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    MultiFieldAdderConfig config = getMacroMultiFieldAdderConfig(NESTED_MACRO_KEY);

    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertEquals(NESTED_MACRO_KEY, config.getMacroFieldValue());
    Assert.assertTrue(config.getFieldValue().isEmpty());
  }

  @Test
  public void testNestedMacroValue() throws NoSuchFieldException {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    MultiFieldAdderConfig config = getMacroMultiFieldAdderConfig(NESTED_MACRO_VALUE);

    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertEquals(NESTED_MACRO_VALUE, config.getMacroFieldValue());
    Assert.assertEquals(MACRO_EXPECTED_VALUES, config.getFieldValue());
  }

  @Test
  public void testPartialMacroKeys() throws NoSuchFieldException {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    MultiFieldAdderConfig config = getMacroMultiFieldAdderConfig(PARTIAL_MACRO_KEYS);

    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertEquals(PARTIAL_MACRO_KEYS, config.getMacroFieldValue());
    Assert.assertEquals(MACRO_EXPECTED_VALUES, config.getFieldValue());
  }

  @Test
  public void testSecureMacro() throws NoSuchFieldException {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    MultiFieldAdderConfig config = getMacroMultiFieldAdderConfig(SECURE_MACRO);

    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertEquals(SECURE_MACRO, config.getMacroFieldValue());
    Assert.assertEquals(MACRO_EXPECTED_VALUES, config.getFieldValue());
  }

  @Test
  public void testWholeMacro() throws NoSuchFieldException {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    MultiFieldAdderConfig config = getMacroMultiFieldAdderConfig(WHOLE_MACRO);

    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
    Assert.assertEquals(WHOLE_MACRO, config.getMacroFieldValue());
    Assert.assertTrue(config.getFieldValue().isEmpty());
  }

  private static MultiFieldAdderConfig getMacroMultiFieldAdderConfig(String macro_value) throws NoSuchFieldException {
    MultiFieldAdderConfig config = MultiFieldAdderConfig.builder().build();

    Set<String> macroFields = new HashSet<>();
    macroFields.add(MultiFieldAdderConfig.FIELD_VALUE);
    Set<String> lookupProperties = new HashSet<>();
    lookupProperties.add("value");
    Map<String,String> properties = new HashMap<>();
    properties.put(MultiFieldAdderConfig.FIELD_VALUE, macro_value);
    Macros macros = new Macros(lookupProperties, null);

    PluginProperties rawProperties = PluginProperties.builder()
            .addAll(properties)
            .build()
            .setMacros(macros);

    FieldSetter.setField(config, MultiFieldAdderConfig.class.getSuperclass().getDeclaredField("rawProperties"),
            rawProperties);
    FieldSetter.setField(config, MultiFieldAdderConfig.class.getSuperclass().getDeclaredField("macroFields"),
            macroFields);

    return config;
  }
}
