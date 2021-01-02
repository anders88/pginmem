package no.anksoft.pginmem.statements

import no.anksoft.pginmem.Column
import no.anksoft.pginmem.DbTransaction
import no.anksoft.pginmem.StatementAnalyzer
import no.anksoft.pginmem.Table
import no.anksoft.pginmem.clauses.MatchAllClause
import no.anksoft.pginmem.clauses.WhereClause
import no.anksoft.pginmem.clauses.createWhereClause
import java.sql.SQLException

class DeleteStatement constructor(statementAnalyzer:StatementAnalyzer, private val dbTransaction: DbTransaction, private val sql:String):StatementWithSet() {
    private val tableForUpdate: Table = dbTransaction.tableForUpdate(statementAnalyzer.addIndex(2).word()?:throw SQLException("Expected tablename"))
    //private val whereClause:WhereClause = if (statementAnalyzer.size > 4  && statementAnalyzer.wordAt(3) == "where") createWhereClause(statementAnalyzer.subList(4,statementAnalyzer.size), listOf(tableForUpdate),1) else MatchAllClause()
    private val whereClause:WhereClause =
        createWhereClause(statementAnalyzer.addIndex(), mapOf(Pair(tableForUpdate.name,tableForUpdate)),1,dbTransaction)

    override fun setSomething(parameterIndex: Int, x: Any?) {
        whereClause.registerBinding(parameterIndex,x)
    }

    override fun executeUpdate(): Int {
        var hits = 0
        val table = Table(tableForUpdate.name,tableForUpdate.colums)
        for (row in tableForUpdate.rowsForReading()) {
            if (whereClause.isMatch(row.cells)) {
                hits++
            } else {
                table.addRow(row)
            }
        }
        dbTransaction.registerTableUpdate(table)
        return hits
    }
}