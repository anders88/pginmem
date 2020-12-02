package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import no.anksoft.pginmem.clauses.MatchAllClause
import no.anksoft.pginmem.clauses.WhereClause
import no.anksoft.pginmem.clauses.createWhereClause
import java.sql.SQLException

private fun <T> wordvalue(list:List<T>, index:Int):T = if (index >= 0 && index < list.size) list[index] else throw SQLException("Unexpected end of statement")


private class CellToUpdate(val column:Column) {
    var value:Any?=null
}



class UpdateStatement(words: List<String>, private val dbTransaction: DbTransaction) : DbPreparedStatement() {
    private val table:Table
    private val whereClause: WhereClause
    private val toUpdate:List<CellToUpdate>

    init {
        table = dbTransaction.tableForUpdate(wordvalue(words,1))
        var ind = 3
        val updates:MutableList<CellToUpdate> = mutableListOf()
        while (true) {
            val column:Column = table.findColumn(wordvalue(words,ind))?:throw SQLException("Unknown column ${wordvalue(words,ind)}")
            updates.add(CellToUpdate(column))
            ind+=3
            if (ind == words.size || wordvalue(words,ind) == "where") {
                break
            }
            if (wordvalue(words,ind) != ",") {
                throw SQLException("Expected , or where")
            }
            ind++
        }
        toUpdate = updates
        whereClause = if (ind < words.size) createWhereClause(words.subList(ind+1,words.size), listOf(table),toUpdate.size+1) else MatchAllClause()
    }

    override fun setString(parameterIndex: Int, x: String?) {
        if (parameterIndex-1 < toUpdate.size) {
            toUpdate[parameterIndex-1].column.columnType.validateValue(x)
            toUpdate[parameterIndex-1].value = x
            return
        }
        whereClause.registerBinding(parameterIndex,x)
    }

    override fun executeUpdate(): Int {
        val newTable = Table(table.name,table.colums)
        var hits = 0
        for (row in table.rowsForReading()) {
            if (!whereClause.isMatch(row.cells)) {
                // No hit -> No change -> keep
                newTable.addRow(row)
                continue
            }
            hits++
            val newCells:MutableList<Cell> = mutableListOf()
            for (cell in row.cells) {
                newCells.add(toUpdate
                    .firstOrNull { it.column.name == cell.column.name}
                    ?.let { Cell(it.column,it.value)}?:cell)
            }
            newTable.addRow(Row(newCells))
        }
        dbTransaction.registerTableUpdate(newTable)
        return hits
    }
}