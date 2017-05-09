# Add Field Transform

Description
-----------

Adds a new field to each record. The field value can either be a new UUID, or it can be set directly through
configuration. This transform is used when you want to add a unique id field to each record, or when you want
to tag each record with some constant value. For example, you may want to add the logical start time as a field
to each record.

Properties
----------

**fieldName:** The name of the field to add. Must not already exist as an input field.
The field type will be a nullable string

**fieldValue:** The value to set for the new field. If this is not specified, 'asUUID' must be set to true. (Macro-enabled)

**asUUID:** Generate a new UUID for the new field. If this is not true, 'fieldValue' must be specified.

Example
-------

This example adds a new field called 'logicalStartTime' that is set to the logical start time of the run:

    {
        "name": "AddField",
        "type": "transform",
        "properties": {
            "fieldName": "logicalStartTime",
            "fieldValue": "${logicalStartTime(yyyy-MM-dd'T'HH-mm-ss)}"
        }
    }

This example adds a new field called 'id' that will is set to a new UUID for each record:

    {
        "name": "AddField",
        "type": "transform",
        "properties": {
            "fieldName": "id",
            "asUUID": "true"
        }
    }
