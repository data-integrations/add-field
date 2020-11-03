package org.example.hydrator.plugin;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class MultiFieldAdderTest {

    private static final Schema INPUT = Schema.recordOf("input",
            Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("last_name", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("country", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("address", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("phone", Schema.of(Schema.Type.STRING)));

    @Test
    public void testMultiFieldAdder()throws Exception{

        MultiFieldAdder.Conf config = new MultiFieldAdder.Conf();


        Transform<StructuredRecord, StructuredRecord> transform = new MultiFieldAdder(config);
        transform.initialize(null);

        MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
        transform.transform(StructuredRecord.builder(INPUT)
                .set("name", "Kimi")
                .set("last_name", "Yusuphi")
                .set("country", "North Macedonia")
                .set("address", "Tetovo")
                .set("phone", "+38970123456").build(), emitter);

        Assert.assertNotEquals("Flakrim", emitter.getEmitted().get(0).get("name"));
        Assert.assertNotEquals("Jusufi", emitter.getEmitted().get(0).get("last_name"));
        Assert.assertEquals("North Macedonia", emitter.getEmitted().get(0).get("country"));
        Assert.assertEquals("Tetovo", emitter.getEmitted().get(0).get("address"));
        Assert.assertEquals("+38970123456", emitter.getEmitted().get(0).get("phone"));
        Assert.assertEquals(5, Objects.requireNonNull(emitter.getEmitted().get(0).getSchema().getFields()).size());

    }
}
