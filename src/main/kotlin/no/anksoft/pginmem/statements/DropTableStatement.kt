package no.anksoft.pginmem.statements

import no.anksoft.pginmem.DbPreparedStatement
import no.anksoft.pginmem.DbTransaction
import no.anksoft.pginmem.StatementAnalyzer
import no.anksoft.pginmem.Table
import java.sql.SQLException

class DropTableStatement(val statementAnalyzer: StatementAnalyzer, private val dbTransaction: DbTransaction): DbPreparedStatement() {
    override fun executeUpdate(): Int {
        statementAnalyzer.addIndex(2)
        val onlyIfExsists = if (statementAnalyzer.word() == "if" && statementAnalyzer.word(1) == "exists") {
            statementAnalyzer.addIndex(2)
            true
        } else false
        val tablename = statementAnalyzer.word()?:throw SQLException("Expected tablename in drop table")
        val table: Table = dbTransaction.tableForUpdateIdOrNull(tablename)
            ?: if (onlyIfExsists) {
                return 0
            } else {
                throw SQLException()
            }
        dbTransaction.removeTable(table)
        return 0
    }
}