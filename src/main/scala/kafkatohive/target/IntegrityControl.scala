package kafkatohive.target

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object IntegrityControl {

  /** Проверка уникальности записей */
  def checkRecordsUnique(df: DataFrame): Boolean = {
    df.drop("dt_load").dropDuplicates().count() == df.count()
  }

  /** Проверка записанных и считанных колонок */
  def checkColumnsNames(df: DataFrame, columns: Array[String]): Boolean = {
    columns.forall(col => df.columns.map(_.toUpperCase).contains(col))
  }

  /** Проверка количества конкретных значений в указанной колонке */
  def countColumnValue(df: DataFrame, column: String, value: Any): Long = {
    df.select(column).where(col(column) === value).count()
  }

}
