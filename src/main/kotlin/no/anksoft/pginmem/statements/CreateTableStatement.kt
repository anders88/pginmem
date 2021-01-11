package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import java.sql.SQLException
import java.sql.Timestamp
import java.time.LocalDateTime


class CreateTableStatement(val statementAnalyzer: StatementAnalyzer,private val dbTransaction: DbTransaction):DbPreparedStatement() {

    override fun executeUpdate(): Int {
        statementAnalyzer.addIndex(2)
        val onlyIfNotExsists:Boolean = if (statementAnalyzer.word() == "if" && statementAnalyzer.word(1) == "not" && statementAnalyzer.word(2) == "exists") {
            statementAnalyzer.addIndex(3)
            true
        } else false
        val name = statementAnalyzer.word()?:throw SQLException("Expecting tablename")

        if (onlyIfNotExsists && dbTransaction.doesTableExsist(name)) {
            return 0
        }

        statementAnalyzer.addIndex(2)

        val columns:MutableList<Column> = mutableListOf()
        while (true) {
            columns.add(Column.create(name,statementAnalyzer,dbTransaction))

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