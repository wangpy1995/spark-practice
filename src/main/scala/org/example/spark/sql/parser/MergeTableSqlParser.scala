package org.example.spark.sql.parser

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.misc.Interval
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.parser.{ParseErrorListener, PostProcessor, UnclosedCommentProcessor}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.example.catalyst.parser.{ExampleBaseParser, SqlBaseLexer}

import scala.collection.JavaConverters.asScalaBufferConverter

private class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume

  override def getSourceName(): String = wrapped.getSourceName

  override def index(): Int = wrapped.index

  override def mark(): Int = wrapped.mark

  override def release(marker: Int): Unit = wrapped.release(marker)

  override def seek(where: Int): Unit = wrapped.seek(where)

  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = wrapped.getText(interval)

  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}

class MergeTableSqlParser(session: SparkSession) extends SparkSqlParser {
  private def initParserBase(parserBase: ExampleBaseParser): Unit = {
    val conf = session.sessionState.conf
    parserBase.addParseListener(PostProcessor)
    parserBase.removeErrorListeners()
    parserBase.addErrorListener(ParseErrorListener)
    parserBase.legacy_setops_precedence_enabled = conf.setOpsPrecedenceEnforced
    parserBase.legacy_exponent_literal_as_decimal_enabled = conf.exponentLiteralAsDecimalEnabled
    parserBase.SQL_standard_keyword_behavior = conf.enforceReservedKeywords
    parserBase.double_quoted_identifiers = conf.doubleQuotedIdentifiers
  }

  override def parsePlan(sqlText: String): LogicalPlan = {
    val charStream: CharStream = new UpperCaseCharStream(CharStreams.fromString(sqlText))
    val lexer = new SqlBaseLexer(charStream)
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)
    val tokenStream = new CommonTokenStream(lexer)
    val parserBase = new ExampleBaseParser(tokenStream)
    initParserBase(parserBase)
    parserBase.addParseListener(UnclosedCommentProcessor(sqlText, tokenStream))
    try {
      val ctx = parserBase.mergeTable()
      withOrigin(ctx, Some(sqlText)) {
        val tableIdentifier = ctx.tableIdentifier()
        val dbName = tableIdentifier.db match {
          case null => None
          case _ => Some(tableIdentifier.db.getText)
        }
        val tableName = tableIdentifier.table.getText
        val partitionSpec = ctx.partitionSpec()
        val partitions = partitionSpec match {
          case null => Map.empty[String, String]
          case _ =>
            partitionSpec.partitionVal().asScala.filter(_.constant() != null).map(p => p.identifier().getText -> p.constant().getText).toMap
        }
        MergeTableCommand(dbName, tableName, partitions)
      }
    } catch {
      case e: Exception =>
        //        e.printStackTrace()
        super.parsePlan(sqlText)
    }
  }
}
