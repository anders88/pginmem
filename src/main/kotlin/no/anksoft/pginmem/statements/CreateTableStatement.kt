package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import java.sql.SQLException
import java.sql.Timestamp
import java.time.LocalDateTime


class CreateTableStatement(val statementAnalyzer: StatementAnalyzer,private val dbTransaction: DbTransaction):DbPreparedStatement() {

    override fun executeUpdate(): Int {
        statementAnalyzer.addIndex(2)
        val name = statementAnalyzer.word()?:throw SQLException("Expecting tablename")
        statementAnalyzer.addIndex(2)

        val columns:MutableList<Column> = mutableListOf()
        while (true) {
            val columnName = statementAnalyzer.word()?:throw SQLException("Expecting column name")
            val colType = statementAnalyzer.word(1)?:throw SQLException("Expecting column type")
            statementAnalyzer.addIndex(2)

            val defaultValue:(()->Any?)? = if (statementAnalyzer.word() == "default") {
                statementAnalyzer.addIndex()
                statementAnalyzer.readConstantValue()
            } else null
            val isNotNull = if (statementAnalyzer.word() == "not" && statementAnalyzer.word(1) == "null") {
                statementAnalyzer.addIndex(2)
                true
            } else false
            columns.add(Column(columnName,colType,defaultValue,isNotNull))

            while (statementAnalyzer.word() != ",") {
                if (statementAnalyzer.word() == ")") {
                    val table = Table(name, columns)
                    dbTransaction.createAlterTableSetup(table)
                    return 0
                }
                statementAnalyzer.addIndex()
            }
            statementAnalyzer.addIndex()
        }

    }
}