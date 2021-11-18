package streaming.dsl.mmlib.fe

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.feature.{DiscretizerFeature, VectorAssembler}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, OperateType, TableAuthResult, TableType}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.{Functions, MllibFunctions}
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.common.form.{Extra, FormParams, KV, Select}
import tech.mlsql.dsl.adaptor.MLMapping
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

class SQLMissingValueProcess(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams with ETAuth {

  def this() = this(BaseParams.randomUID())

  private def fillByRandomForestRegressor(df: DataFrame, params: Map[String, String], processColArrays: Array[String]): DataFrame = {
    val regressor = new RandomForestRegressor()
    configureModel(regressor, params)
    val fullColumns = df.columns
    var data = df
    processColArrays.foreach(processCol => {
      val featureCols = fullColumns.filter(c => {
        c != processCol
      })
      val unknown_data = data.filter(col(processCol).isNull)
      val known_data = data.filter(col(processCol).isNotNull)
      val featureName = "features"
      val assembler = new VectorAssembler()
        .setInputCols(featureCols)
        .setOutputCol(featureName)
      val trainData = assembler.transform(known_data)
      val testData = assembler.transform(unknown_data)
      val regressor = new RandomForestRegressor()
        .setLabelCol(processCol)
        .setFeaturesCol(featureName)
        .setPredictionCol("predicted")
      val model = regressor.fit(trainData)
      val predictedData = model.transform(testData)
      // Fillout the misisng value by the predict
      val result = predictedData.withColumn(processCol,
        when(col(processCol).isNull, col("predicted")).otherwise(col(processCol))
      ).drop("predicted").drop("features")
      data = known_data.union(result)
    })
    data
  }

  private def dropWithMissingValue(df: DataFrame, params: Map[String, String]): DataFrame = {
    var data = df
    val columns = df.columns
    columns.foreach(colName => {
      data = data.filter(col(colName).isNotNull)
    })
    data
  }

  private def fillByMeanValue(df: DataFrame, params: Map[String, String], processColumns: Array[String]): DataFrame = {
    import org.apache.spark.sql.functions._
    var result = df
    processColumns.foreach(colName => {
      val meanValue = df.agg(mean(col(colName)).alias("mean")).collect()(0).get(0)
      result = result.withColumn(colName, when(col(colName).isNull, meanValue).otherwise(col(colName)))
    })
    result
  }

  private def fillByModeValue(df: DataFrame, params: Map[String, String], processColumns: Array[String]): DataFrame = {
    import org.apache.spark.sql.functions._
    var result = df
    processColumns.foreach(colName => {
      val modeValue = df.select(colName).groupBy(colName).count().orderBy(df(colName).desc).collect()(0).get(0)
      result = result.withColumn(colName, when(col(colName).isNull, modeValue).otherwise(col(colName)))
    })
    result
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val processColumns = params.getOrElse("processColumns", "").split(",")
    val method = params.getOrElse("method", "mean")
    val data = method match {
      case "mean" => fillByMeanValue(df, params, processColumns)
      case "mode" => fillByModeValue(df, params, processColumns)
      case "drop" => dropWithMissingValue(df, params)
      case "randomforest" => fillByRandomForestRegressor(df, params, processColumns)
    }
    data
  }

  val method:Param[String] = new Param[String](this, "method", FormParams.toJson(
    Select(
      name = "method",
      values = List(),
      extra = Extra(
        doc = "",
        label = "",
        options = Map(
        )), valueProvider = Option(() => {
        List(
          KV(Some("method"), Some(DiscretizerFeature.BUCKETIZER_METHOD)),
          KV(Some("method"), Some(DiscretizerFeature.QUANTILE_METHOD))
        )
      })
    )
  ))

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__fe_missingvalue_process_operator__"),
      OperateType.SELECT,
      Option("select"),
      TableType.SYSTEM)

    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.getTableAuth match {
      case Some(tableAuth) =>
        tableAuth.auth(List(vtable))
      case None =>
        List(TableAuthResult(granted = true, ""))
    }
  }


}
