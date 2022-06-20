@Add_Field
Feature: AddField Plugin - Verify macro scenarios

  @FILE_SOURCE_TEST @BQ_SINK_TEST
  Scenario: Verify add field functionality with macro arguments using File to BigQuery pipeline
    Given Open Datafusion Project to configure pipeline
    And Select plugin: "File" from the plugins list as: "Source"
    And Expand Plugin group in the LHS plugins list: "Transform"
    And Select plugin: "Add Field" from the plugins list as: "Transform"
    And Expand Plugin group in the LHS plugins list: "Sink"
    And Select plugin: "BigQuery" from the plugins list as: "Sink"
    And Connect plugins: "File" and "AddField" to establish connection
    And Connect plugins: "AddField" and "BigQuery" to establish connection
    And Navigate to the properties page of plugin: "File"
    And Enter input plugin property: "referenceName" with value: "FileReferenceName"
    And Enter input plugin property: "path" with value: "csvAllDataTypeFile"
    And Select dropdown plugin property: "format" with option value: "csv"
    And Click plugin property: "skipHeader"
    And Click on the Get Schema button
    And Validate "File" plugin properties
    And Close the Plugin Properties page
    And Navigate to the properties page of plugin: "AddField"
    And Enter input plugin property: "addFieldFieldName" with value: "afFieldName"
    And Click on the Macro button of Property: "addFieldFieldValue" and set the value to: "fieldvalue"
    And Select dropdown plugin property: "addFieldGenerateUUID" with option value: "false"
    And Validate "AddField" plugin properties
    And Validate output schema with expectedSchema "csvAllDataTypeFileSchemaAddField"
    And Close the Plugin Properties page
    And Navigate to the properties page of plugin: "BigQuery"
    And Replace input plugin property: "projectId" with value: "projectId"
    And Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Override Service account details if set in environment variables
    And Enter input plugin property: "referenceName" with value: "BQReferenceName"
    And Enter input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqTargetTable"
    And Click plugin property: "truncateTable"
    And Click plugin property: "updateTableSchema"
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    And Preview and run the pipeline
    And Enter runtime argument value "afFieldValue" for key "fieldvalue"
    And Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    And Deploy the pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "afFieldValue" for key "fieldvalue"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate OUT record count is equal to IN record count
    Then Verify column: "afFieldName" is added in target BigQuery table: "bqTargetTable"
