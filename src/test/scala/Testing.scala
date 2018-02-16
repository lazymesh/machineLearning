
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterEach, FunSuite}


/**
  * Created by hadoop on 11/10/17.
  */
class Testing extends FunSuite with BeforeAndAfterEach{

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterEach() : Unit = {
    super.afterEach()
    spark.stop
  }

  test("oooo"){

    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext


//    val df = Seq(
//    (1, Array("A", "B"), Array(1, 2))
//    ).toDF("ID", "ArraY", "array2")
//

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    val dfContentEnvelope = sqlContext.read.format("com.databricks.spark.xml")
      .option("rowTag", "env:ContentEnvelope")
      .load("/home/hadoop/IdeaProjects/AscolSbt/src/test/resources/tt.xml")

//    dfContentEnvelope.printSchema()
//
    val getDataPartition =  udf { (DataPartition: Long) =>
      if (DataPartition== 1) "SelfSourcedPublic"
      else  if (DataPartition== 2) "Japan"
      else  if (DataPartition== 3) "SelfSourcedPrivate"
      else "ThirdPartyPrivate"
    }

    val getFFActionParent =  udf { (FFAction: String) =>
      if (FFAction=="Insert") "I|!|"
      else if (FFAction=="Overwrite") "I|!|"
      else "D|!|"
    }

    val getFFActionChild =  udf { (FFAction: String) =>
      if (FFAction=="Insert") "I|!|"
      else if (FFAction=="Overwrite") "O|!|"
      else "D|!|"
    }
    val header1 = """Source.organizationId|^|Source.sourceId|^|FilingDateTime|^|SourceTypeCode|^|DocumentId|^|Dcn|^|DocFormat|^|StatementDate|^|IsFilingDateTimeEstimated|^|ContainsPreliminaryData|^|CapitalChangeAdjustmentDate|^|CumulativeAdjustmentFactor|^|ContainsRestatement|^|FilingDateTimeUTCOffset|^|ThirdPartySourceCode|^|ThirdPartySourcePriority|^|SourceTypeId|^|ThirdPartySourceCodeId|^|FFAction|!|"""
    val schema = StructType(header1.split("\\|\\^\\|").map(cols => StructField(cols.replace(".", "_"), StringType)).toSeq)

    val dfContentItem = dfContentEnvelope.withColumn("column1", explode(dfContentEnvelope("env:Body.env:ContentItem"))).select($"env:Header.fun:DataPartitionId".as("DataPartitionId"),$"column1.*")
    val dfType=dfContentItem.select(getDataPartition($"DataPartitionId"), $"env:Data.sr:Source.*", $"_action".as("FFAction|!|")).filter($"env:Data.sr:Source._organizationId".isNotNull)

    val temp = dfType.select(dfType.columns.filter(x => !x.equals("sr:Auditors")).map(x => col(x).as(x.replace("_", "Source_").replace("sr:", ""))): _*)
    val diff = schema.fieldNames.diff(temp.schema.fieldNames)
    val finaldf = diff.foldLeft(temp){(temp2df, colName) => temp2df.withColumn(colName, lit(null))}

    val ParentDF=dfType.select($"_organizationId".as("organizationId"), $"_sourceId".as("sourceId"), explode($"sr:Auditors.sr:Auditor").as("Auditors"), getFFActionChild($"FFAction|!|").as("FFAction|!|"))
    ParentDF.select($"organizationId", $"sourceId", $"Auditors.*", $"FFAction|!|").show(false)
//    finaldf.show(false)

    val headerColumn = schema.fieldNames.toSeq
    val header = headerColumn.mkString("", "|^|", "")

    val finaldfWithDelimiter=finaldf.select(concat_ws("|^|",finaldf.schema.fieldNames.map(col): _*).as("concatenated")).withColumnRenamed("concatenated", header)
    finaldfWithDelimiter.show(false)
  }

  test("test") {
    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext

    val df = sqlContext.read.option("delimiter", " ").csv("/home/hadoop/IdeaProjects/AscolSbt/src/test/resources/test.csv")
    df.show(false)
  }

}

case class Merged(value : Int, annotations: Annotation)
case class Annotation(field1: String, field2: String, field3: Int, field4: Float, field5: Int, field6: List[Mapping])
case class Mapping(fieldA: String, fieldB: String, fieldC: String, fieldD: String, fieldE: String)

