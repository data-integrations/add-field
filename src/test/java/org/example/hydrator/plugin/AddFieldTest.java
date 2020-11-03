/*
 * Copyright © 2017 Cask Data, Inc.
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
package org.example.hydrator.plugin;


import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import org.junit.Assert;
import org.junit.Test;
import java.util.UUID;

import java.util.Objects;

/**
 * This is an example of how you can build unit tests for your transform.
 */
public class AddFieldTest {
    private static final Schema INPUT = Schema.recordOf("input",
            Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("last_name", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("country", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("address", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("phone", Schema.of(Schema.Type.STRING)));
    @Test
    public void testAddingNewField() throws Exception {
        AddField.Conf config = new AddField.Conf();
        config.fieldName = "My_New_Field";
        config.fieldValue = "My_New_Value";
        Transform<StructuredRecord, StructuredRecord> transform = new AddField(config);
        transform.initialize(null);

        MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
        transform.transform(StructuredRecord.builder(INPUT)
                .set("name", "Flakrim")
                .set("last_name", "Jusufi")
                .set("country", "North Macedonia")
                .set("address", "Tetovo")
                .set("phone", "+38970123456").build(), emitter);
        Assert.assertEquals("Flakrim", emitter.getEmitted().get(0).get("name"));
        Assert.assertEquals("Jusufi", emitter.getEmitted().get(0).get("last_name"));
        Assert.assertEquals("North Macedonia", emitter.getEmitted().get(0).get("country"));
        Assert.assertEquals("Tetovo", emitter.getEmitted().get(0).get("address"));
        Assert.assertEquals("+38970123456", emitter.getEmitted().get(0).get("phone"));
        Assert.assertEquals(6, Objects.requireNonNull(emitter.getEmitted().get(0).getSchema().getFields()).size());

    }

    private static boolean isUUID(String string) {
        try {
            UUID.fromString(string);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    @Test
    public void testDefaultField()throws Exception{

        AddField.Conf config = new AddField.Conf();
        config.fieldName = "UDD_Field";
        config.asUUID = true;
        Transform<StructuredRecord, StructuredRecord> transform = new AddField(config);
        transform.initialize(null);

        MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
        transform.transform(StructuredRecord.builder(INPUT)
                .set("name", "Flakrim")
                .set("last_name", "Jusufi")
                .set("country", "North Macedonia")
                .set("address", "Tetovo")
                .set("phone", "+38970123456").build(), emitter);

        Assert.assertEquals("Flakrim", emitter.getEmitted().get(0).get("name"));
        Assert.assertEquals("Jusufi", emitter.getEmitted().get(0).get("last_name"));
        Assert.assertEquals("North Macedonia", emitter.getEmitted().get(0).get("country"));
        Assert.assertEquals("Tetovo", emitter.getEmitted().get(0).get("address"));
        Assert.assertEquals("+38970123456", emitter.getEmitted().get(0).get("phone"));
        Assert.assertTrue(isUUID(emitter.getEmitted().get(0).get("UDD_Field")));
        Assert.assertEquals(6, Objects.requireNonNull(emitter.getEmitted().get(0).getSchema().getFields()).size());

    }
}