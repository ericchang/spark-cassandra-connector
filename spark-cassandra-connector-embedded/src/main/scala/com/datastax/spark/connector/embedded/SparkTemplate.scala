package com.datastax.spark.connector.embedded

import java.io.File

import akka.actor.ActorSystem
import com.datastax.spark.connector.embedded.EmbeddedCassandra._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

trait SparkTemplate {
  /** Obtains the active [[org.apache.spark.SparkContext SparkContext]] object. */
  def sc: SparkContext = SparkTemplate.sc

  /** Ensures that the currently running [[org.apache.spark.SparkContext SparkContext]] uses the provided
    * configuration. If the configurations are different or force is `true` Spark context is stopped and
    * started again with the given configuration. */
  def useSparkConf(conf: SparkConf, force: Boolean = false): SparkContext =
    SparkTemplate.useSparkConf(conf, force)

  def defaultSparkConf = SparkTemplate.defaultConf
}

object SparkTemplate {

  /** Default configuration for [[org.apache.spark.SparkContext SparkContext]]. */
  val defaultConf = new SparkConf(true)
    .set("spark.cassandra.connection.host", getHost(0).getHostAddress)
    .set("spark.cassandra.connection.port", getPort(0).toString)
    .set("spark.ui.enabled", "false")
    .set("spark.cleaner.ttl", "3600")
    .setMaster(sys.env.getOrElse("IT_TEST_SPARK_MASTER", "local[*]"))
    .setAppName("Test")

  private var _sc: SparkContext = _

  private var _conf: SparkConf = _
  /** Ensures that the currently running [[org.apache.spark.SparkContext SparkContext]] uses the provided
    * configuration. If the configurations are different or force is `true` Spark context is stopped and
    * started again with the given configuration. */
  def useSparkConf(conf: SparkConf = SparkTemplate.defaultConf, force: Boolean = false): SparkContext = {
    if (_conf.getAll.toMap != conf.getAll.toMap || force)
      resetSparkContext(conf)
    _sc
  }

  private def resetSparkContext(conf: SparkConf) = {
    if (_sc != null) {
      _sc.stop()
    }

    System.err.println("Starting SparkContext with the following configuration:\n" + defaultConf.toDebugString)
    _conf = conf.clone()
    for (cp <- sys.env.get("SPARK_SUBMIT_CLASSPATH"))
      conf.setJars(
        cp.split(File.pathSeparatorChar)
          .filter(_.endsWith(".jar"))
          .map(new File(_).getAbsoluteFile.toURI.toString))
    _sc = new SparkContext(conf)
    _sc
  }

  /** Obtains the active [[org.apache.spark.SparkContext SparkContext]] object. */
  def sc: SparkContext = _sc

  def withoutLogging[T]( f: => T): T={
    val level = Logger.getRootLogger.getLevel
    Logger.getRootLogger.setLevel(Level.OFF)
    val ret = f
    Logger.getRootLogger.setLevel(level)
    ret
  }

  resetSparkContext(defaultConf)
}
