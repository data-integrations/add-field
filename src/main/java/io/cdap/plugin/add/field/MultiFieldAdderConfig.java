package io.cdap.plugin.add.field;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.Map;
import java.util.TreeMap;

public class MultiFieldAdderConfig extends PluginConfig {
  public static final String FIELD_VALUE = "fieldValue";

  @Name(FIELD_VALUE)
  @Macro
  @Description("Specify a field value pair that needs to added to output.")
  private String fieldValue;

  public MultiFieldAdderConfig() {
  }

  public MultiFieldAdderConfig(String fieldValue) {
    this.fieldValue = fieldValue;
  }

  private MultiFieldAdderConfig(Builder builder) {
    fieldValue = builder.fieldValue;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(MultiFieldAdderConfig copy) {
    return builder()
      .setFieldValue(copy.fieldValue);
  }

  Map<String, String> getFieldValue() throws IllegalArgumentException {
    Map<String, String> values = new TreeMap<>();
    if (containsMacro(FIELD_VALUE) || fieldValue == null || fieldValue.trim().isEmpty()) {
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
      values.put(fieldName, fieldValue);
    }
    return values;
  }

  public void validate(FailureCollector failureCollector) {
    if (!containsMacro(FIELD_VALUE)){
      Map<String, String> values = new TreeMap<>();
      String[] fvPairs = fieldValue.split(",");
      for (String fvPair : fvPairs) {
        String[] fieldAndValue = fvPair.split(":");
        if (fieldAndValue.length != 2) {
          continue;
        }
        String fieldName = fieldAndValue[0];
        String fieldValue = fieldAndValue[1];
        if (values.containsKey(fieldName)) {
          failureCollector.addFailure(String.format("Field '%s' is specified multiple times.", fieldName),
                                      "Each field must be specified only once.")
            .withConfigProperty(FIELD_VALUE);
          break;
        }
        values.put(fieldName, fieldValue);
      }
    }
  }

  /**
   * Builder for MultiFieldAdderConfig
   */
  public static final class Builder {
    private String fieldValue;

    private Builder() {
    }

    public static Builder aMultiFieldAdderConfig() {
      return new Builder();
    }

    public Builder setFieldValue(String fieldValue) {
      this.fieldValue = fieldValue;
      return this;
    }

    public MultiFieldAdderConfig build() {
      return new MultiFieldAdderConfig(this);
    }
  }
}
