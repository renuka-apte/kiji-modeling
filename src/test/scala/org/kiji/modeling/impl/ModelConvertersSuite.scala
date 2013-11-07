/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.modeling.impl

import com.twitter.scalding.Source
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import org.kiji.express.flow.AndFilter
import org.kiji.express.flow.ColumnRangeFilter
import org.kiji.express.flow.ExpressColumnFilter
import org.kiji.express.flow.OrFilter
import org.kiji.express.flow.RegexQualifierFilter
import org.kiji.modeling.ExtractFn
import org.kiji.modeling.Extractor
import org.kiji.modeling.Preparer
import org.kiji.modeling.ScoreFn
import org.kiji.modeling.Scorer
import org.kiji.modeling.Trainer
import org.kiji.modeling.config.ExpressColumnRequest
import org.kiji.modeling.config.ExpressDataRequest
import org.kiji.modeling.config.FieldBinding
import org.kiji.modeling.config.InputSpec
import org.kiji.modeling.config.KeyValueStoreSpec
import org.kiji.modeling.config.KijiInputSpec
import org.kiji.modeling.config.KijiOutputSpec
import org.kiji.modeling.config.KijiSingleColumnOutputSpec
import org.kiji.modeling.config.ModelDefinition
import org.kiji.modeling.config.ModelEnvironment
import org.kiji.modeling.config.OutputSpec
import org.kiji.modeling.config.PrepareEnvironment
import org.kiji.modeling.config.ScoreEnvironment
import org.kiji.modeling.config.SequenceFileSourceSpec
import org.kiji.modeling.config.TextSourceSpec
import org.kiji.modeling.config.TrainEnvironment
import org.kiji.modeling.framework.ModelConverters

trait SerDeSuite extends FunSuite {
  def serDeTest[I](inputName: String, serdeName: String, input: => I)(operation: I => I) {
    test("conversion of a %s to/from %s".format(inputName, serdeName)) {
      val expected: I = input
      val actual: I = operation(expected)

      assert(expected === actual)
    }
  }
}

@RunWith(classOf[JUnitRunner])
class ModelConvertersSuite extends SerDeSuite {
  import ModelConvertersSuite._

  serDeTest("ModelDefinition", "Avro", testModelDefinition) { input =>
    ModelConverters.modelDefinitionFromAvro(ModelConverters.modelDefinitionToAvro(input))
  }

  serDeTest("ModelEnvironment", "Avro", testModelEnvironment) { input =>
    ModelConverters.modelEnvironmentFromAvro(ModelConverters.modelEnvironmentToAvro(input))
  }

  serDeTest("PrepareEnvironment", "Avro", testPrepareEnvironment) { input =>
    ModelConverters.prepareEnvironmentFromAvro(ModelConverters.prepareEnvironmentToAvro(input))
  }

  serDeTest("TrainEnvironment", "Avro", testTrainEnvironment) { input =>
    ModelConverters.trainEnvironmentFromAvro(ModelConverters.trainEnvironmentToAvro(input))
  }

  serDeTest("ScoreEnvironment", "Avro", testScoreEnvironment) { input =>
    ModelConverters.scoreEnvironmentFromAvro(ModelConverters.scoreEnvironmentToAvro(input))
  }

  serDeTest[InputSpec]("KijiInputSpec", "Avro", testKijiInputSpec) { input =>
    ModelConverters.inputSpecFromAvro(ModelConverters.inputSpecToAvro(input))
  }

  serDeTest[InputSpec]("TextInputSpec", "Avro", testTextSpec) { input =>
    ModelConverters.inputSpecFromAvro(ModelConverters.inputSpecToAvro(input))
  }

  serDeTest[InputSpec]("SequenceFileInputSpec", "Avro", testSequenceFileSpec) { input =>
    ModelConverters.inputSpecFromAvro(ModelConverters.inputSpecToAvro(input))
  }

  serDeTest[OutputSpec]("KijiOutputSpec", "Avro", testKijiOutputSpec) { input =>
    ModelConverters.outputSpecFromAvro(ModelConverters.outputSpecToAvro(input))
  }

  serDeTest[OutputSpec]("ColumnOutputSpec", "Avro", testColumnOutputSpec) { input =>
    ModelConverters.outputSpecFromAvro(ModelConverters.outputSpecToAvro(input))
  }

  serDeTest[OutputSpec]("TextOutputSpec", "Avro", testTextSpec) { input =>
    ModelConverters.outputSpecFromAvro(ModelConverters.outputSpecToAvro(input))
  }

  serDeTest[OutputSpec]("SequenceFileOutputSpec", "Avro", testSequenceFileSpec) { input =>
    ModelConverters.outputSpecFromAvro(ModelConverters.outputSpecToAvro(input))
  }

  serDeTest("DataRequest", "Avro",testDataRequest) { input =>
    ModelConverters.dataRequestFromAvro(ModelConverters.dataRequestToAvro(input))
  }

  serDeTest("KVStore", "Avro",testKVStore) { input =>
    ModelConverters.keyValueStoreSpecFromAvro(ModelConverters.keyValueStoreSpecToAvro(input))
  }

  serDeTest("FieldBinding", "Avro",testFieldBinding) { input =>
    ModelConverters.fieldBindingFromAvro(ModelConverters.fieldBindingToAvro(input))
  }

  serDeTest[ExpressColumnFilter]("AndFilter", "Avro", testAndFilter) { input =>
    ModelConverters.filterFromAvro(ModelConverters.filterToAvro(input))
  }

  serDeTest[ExpressColumnFilter]("OrFilter", "Avro", testOrFilter) { input =>
    ModelConverters.filterFromAvro(ModelConverters.filterToAvro(input))
  }

  serDeTest[ExpressColumnFilter]("RangeFilter", "Avro", testRangeFilter) { input =>
    ModelConverters.filterFromAvro(ModelConverters.filterToAvro(input))
  }

  serDeTest[ExpressColumnFilter]("RegexFilter", "Avro", testRegexFilter) { input =>
    ModelConverters.filterFromAvro(ModelConverters.filterToAvro(input))
  }
}

object ModelConvertersSuite {
  // scalastyle:off null
  class TestExtractor extends Extractor {
    override def extractFn: ExtractFn[_, _] = { null }
  }
  class TestPreparer extends Preparer {
    override def prepare(inputs: Map[String, Source], outputs: Map[String, Source]): Boolean = {
      true }
  }
  class TestTrainer extends Trainer {
    override def train(inputs: Map[String, Source], outputs: Map[String,
        Source]): Boolean = { true }
  }
  class TestScorer extends Scorer {
    override def scoreFn: ScoreFn[_, _] = { null }
  }
  // scalastyle:on null

  val testRangeFilter: ColumnRangeFilter = ColumnRangeFilter(
      minimum = Some("0min"),
      maximum = Some("9max"),
      minimumIncluded = false,
      maximumIncluded = true)
  val testRegexFilter: RegexQualifierFilter = RegexQualifierFilter(".*")
  val testAndFilter: AndFilter = AndFilter(Seq(testRangeFilter, testRegexFilter))
  val testOrFilter: OrFilter = OrFilter(Seq(testRangeFilter, testRegexFilter))
  val testFieldBinding: FieldBinding = FieldBinding("testField", "info:test")
  val testKVStore: KeyValueStoreSpec = KeyValueStoreSpec(
      storeType = "KIJI_TABLE",
      name = "testkvstore",
      properties = Map(
          "uri" -> "kiji://.env/default/test4",
          "column" -> "info:test"))
  val testDataRequest: ExpressDataRequest = ExpressDataRequest(
      minTimestamp = 0L,
      maxTimestamp = Long.MaxValue - 1,
      columnRequests = Seq(ExpressColumnRequest("info:test", 1, Some(testAndFilter))))
  val testKijiInputSpec: KijiInputSpec = KijiInputSpec(
      tableUri = "kiji://.env/default/test",
      dataRequest = testDataRequest,
      fieldBindings = Seq(testFieldBinding))
  val testTextSpec: TextSourceSpec = TextSourceSpec(
      path = "hdfs://test")
  val testSequenceFileSpec: SequenceFileSourceSpec = SequenceFileSourceSpec(
      path = "hdfs://test",
      keyField = Some("key"),
      valueField = Some("value"))
  val testKijiOutputSpec: KijiOutputSpec = KijiOutputSpec(
      tableUri = "kiji://.env/default/test2",
      fieldBindings = Seq(testFieldBinding))
  val testColumnOutputSpec: KijiSingleColumnOutputSpec = KijiSingleColumnOutputSpec(
      tableUri = "kiji://.env/default/test3",
      outputColumn = "info:test")
  val testPrepareEnvironment: PrepareEnvironment = PrepareEnvironment(
      inputSpec = Map("input" -> testKijiInputSpec),
      outputSpec = Map("output" -> testKijiOutputSpec),
      keyValueStoreSpecs = Seq(testKVStore))
  val testTrainEnvironment: TrainEnvironment = TrainEnvironment(
      inputSpec = Map("input" -> testKijiInputSpec),
      outputSpec = Map("output" -> testKijiOutputSpec),
      keyValueStoreSpecs = Seq(testKVStore))
  val testScoreEnvironment: ScoreEnvironment = ScoreEnvironment(
      inputSpec = testKijiInputSpec,
      outputSpec = testColumnOutputSpec,
      keyValueStoreSpecs = Seq(testKVStore))
  val testModelEnvironment: ModelEnvironment = ModelEnvironment(
      name = "test",
      version = "1.0.0",
      prepareEnvironment = Some(testPrepareEnvironment),
      trainEnvironment = Some(testTrainEnvironment),
      scoreEnvironment = Some(testScoreEnvironment))
  val testModelDefinition: ModelDefinition = ModelDefinition(
      name = "test",
      version = "1.0.0",
      preparerClass = Some(classOf[TestPreparer]),
      trainerClass = Some(classOf[TestTrainer]),
      scoreExtractorClass = Some(classOf[TestExtractor]),
      scorerClass = Some(classOf[TestScorer]))
}
