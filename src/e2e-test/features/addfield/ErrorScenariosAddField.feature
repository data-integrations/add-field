@Add_Field
Feature: AddField Plugin - Verify error scenarios

  @ADD_FIELD-01
  Scenario: Verify add field validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    And Expand Plugin group in the LHS plugins list: "Transform"
    And Select plugin: "Add Field" from the plugins list as: "Transform"
    And Navigate to the properties page of plugin: "AddField"
    And Click on the Validate button
    And Verify mandatory property error for below listed properties:
      | fieldName |

  @ADD_FIELD-02
  Scenario: Validate invalid error messages in add field plugin without any input data
    Given Open Datafusion Project to configure pipeline
    And Expand Plugin group in the LHS plugins list: "Transform"
    And Select plugin: "Add Field" from the plugins list as: "Transform"
    And Navigate to the properties page of plugin: "AddField"
    And Enter input plugin property: "addFieldFieldName" with value: "afFieldName"
    And Click on the Get Schema button
    And Verify that the Plugin Property: "fieldValue" is displaying an in-line error message: "addFieldErrorMessageInvalidField"
    And Verify that the Plugin Property: "asUUID" is displaying an in-line error message: "addFieldErrorMessageInvalidField"

  @ADD_FIELD-03
  Scenario: Verify error count for add field plugin for mandatory fields
    Given Open Datafusion Project to configure pipeline
    And Expand Plugin group in the LHS plugins list: "Transform"
    And Select plugin: "Add Field" from the plugins list as: "Transform"
    And Navigate to the properties page of plugin: "AddField"
    And Click on the Validate button
    And Verify plugin properties validation fails with 1 error

  @ADD_FIELD-04
  Scenario: Validate errors when field value is given and generate uuid as value is set to true
    Given Open Datafusion Project to configure pipeline
    And Expand Plugin group in the LHS plugins list: "Transform"
    And Select plugin: "Add Field" from the plugins list as: "Transform"
    And Navigate to the properties page of plugin: "AddField"
    And Enter input plugin property: "addFieldFieldName" with value: "afFieldName"
    And Enter input plugin property: "addFieldFieldValue" with value: "afFieldValue"
    And Select dropdown plugin property: "addFieldGenerateUUID" with option value: "true"
    And Click on the Get Schema button
    And Verify that the Plugin Property: "fieldValue" is displaying an in-line error message: "addFieldErrorMessageValidFieldValue"
    And Verify that the Plugin Property: "asUUID" is displaying an in-line error message: "addFieldErrorMessageValidFieldValue"
