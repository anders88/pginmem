package no.anksoft.pginmem.statements

import no.anksoft.pginmem.Column
import no.anksoft.pginmem.DbTransaction
import no.anksoft.pginmem.Table
import no.anksoft.pginmem.clauses.MatchAllClause
import no.anksoft.pginmem.clauses.WhereClause
import no.anksoft.pginmem.clauses.createWhereClause

class DeleteStatement constructor(words: List<String>, private val dbTransaction: DbTransaction, private val sql:String):StatementWithSet() {
    private val tableForUpdate: Table = dbTransaction.tableForUpdate(words[2])
    private val whereClause:WhereClause = if (words.size > 4  && words[3] == "where") createWhereClause(words.subList(4,words.size), listOf(tableForUpdate),1) else MatchAllClause()

    override fun setSomething(parameterIndex: Int, x: Any?) {
        whereClause.registerBinding(parameterIndex,x)
    }

    override fun executeUpdate(): Int {
        var hits = 0
        val table = Table(tableForUpdate.name,tableForUpdate.colums)
        for (row in tableForUpdate.rowsForReading()) {
            if (whereClause.isMatch(row.cells)) {
                hits
            } else {
                table.addRow(row)
            }
        }
        dbTransaction.registerTableUpdate(table)
        return hits
    }
}