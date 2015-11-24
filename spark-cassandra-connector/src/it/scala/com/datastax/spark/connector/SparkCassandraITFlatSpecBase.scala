package com.datastax.spark.connector

import java.util.concurrent.Executors

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

import org.scalatest._

import com.datastax.driver.core.Session
import com.datastax.spark.connector.embedded.SparkTemplate
import com.datastax.spark.connector.testkit.{AbstractSpec, SharedEmbeddedCassandra}


trait SparkCassandraITFlatSpecBase extends FlatSpec with SparkCassandraITSpecBase

trait SparkCassandraITWordSpecBase extends WordSpec with SparkCassandraITSpecBase

trait SparkCassandraITAbstractSpecBase extends AbstractSpec with SparkCassandraITSpecBase

trait SparkCassandraITSpecBase extends Suite with Matchers with SharedEmbeddedCassandra with SparkTemplate with BeforeAndAfterAll {

  val clName = ""
  val ks = "scc_test_" + clName

  implicit val ec = SparkCassandraITSpecBase.ec

  def doAsync(units: Future[Unit]*): Unit = {
    implicit val ec = scala.concurrent.ExecutionContext.global
    Await.result(Future.sequence(units), Duration.Inf)
  }

  def keyspaceCql(name: String = ks) =
    s"""
       |CREATE KEYSPACE IF NOT EXISTS $name
       |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
       |""".stripMargin

  def createKeyspace(session: Session, name: String = ks): Unit = {
    session.execute(s"DROP KEYSPACE IF EXISTS $name")
    session.execute(keyspaceCql(name))
  }

}

object SparkCassandraITSpecBase {
  val executor = Executors.newFixedThreadPool(100)
  val ec = ExecutionContext.fromExecutor(executor)
}
