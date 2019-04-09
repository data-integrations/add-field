/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.plugin;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.AddField;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for our plugins.
 */
public class PipelineTest extends HydratorTestBase {
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "1.0.0");
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the data-pipeline artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the data-pipeline artifact as its parent.
    // this will make our plugins available to data-pipeline.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact, AddField.class);
  }

  @Test
  public void testStringCaseTransform() throws Exception {
    String inputName = "transformTestInput";
    String outputName = "transformTestOutput";

    // create the pipeline config
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputName));
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputName));
    Map<String, String> transformProperties = new HashMap<>();
    transformProperties.put("fieldName", "newField");
    transformProperties.put("fieldValue", "${val}");
    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin(AddField.NAME, Transform.PLUGIN_TYPE,
                                                    transformProperties, null));
    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addStage(transform)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    // create the pipeline
    ApplicationId pipelineId = NamespaceId.DEFAULT.app("transformTestPipeline");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, pipelineConfig));

    // write the input
    Schema schema = Schema.recordOf("name", Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    DataSetManager<Table> inputManager = getDataset(inputName);
    List<StructuredRecord> inputRecords = new ArrayList<>();
    inputRecords.add(StructuredRecord.builder(schema).set("name", "samuel").build());
    MockSource.writeInput(inputManager, inputRecords);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start(ImmutableMap.of("val", "abc123"));
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 4, TimeUnit.MINUTES);

    Schema outputSchema = Schema.recordOf(
      "name.added",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("newField", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    DataSetManager<Table> outputManager = getDataset(outputName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    List<StructuredRecord> expected = new ArrayList<>();
    expected.add(StructuredRecord.builder(outputSchema).set("name", "samuel").set("newField", "abc123").build());
    Assert.assertEquals(expected, outputRecords);
  }
}
