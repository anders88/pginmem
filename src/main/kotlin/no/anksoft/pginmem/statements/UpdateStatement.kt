package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import no.anksoft.pginmem.clauses.MatchAllClause
import no.anksoft.pginmem.clauses.WhereClause
import no.anksoft.pginmem.clauses.createWhereClause
import java.sql.SQLException
import java.sql.Timestamp

private fun <T> wordvalue(list:List<T>, index:Int):T = if (index >= 0 && index < list.size) list[index] else throw SQLException("Unexpected end of statement")


private class CellToUpdate(val column:Column) {
    var value:Any?=null
}



class UpdateStatement(statementAnalyzer: StatementAnalyzer, private val dbTransaction: DbTransaction) : StatementWithSet() {
    private val table:Table
    private val whereClause: WhereClause
    private val toUpdate:List<CellToUpdate>

    init {

        table = dbTransaction.tableForUpdate(statementAnalyzer.addIndex(1).word()?:throw SQLException("Unexpected end of statement") )
        statementAnalyzer.addIndex(2)
        val updates:MutableList<CellToUpdate> = mutableListOf()
        while (true) {
            val colnameToUpdate = statementAnalyzer.word() ?: throw SQLException("Unexpected end of statement")
            val column:Column = table.findColumn(colnameToUpdate)?:throw SQLException("Unknown column ${statementAnalyzer.word()}")
            updates.add(CellToUpdate(column))
            if (statementAnalyzer.addIndex().word() != "=") {
                throw SQLException("Expected =")
            }
            if (statementAnalyzer.addIndex().word() != "?") {
                throw SQLException("Expected ?")
            }
            statementAnalyzer.addIndex()
            if (statementAnalyzer.word()?:"where" == "where") {
                break
            }
            if (statementAnalyzer.word() != ",") {
                throw SQLException("Expected where or ,")
            }
            statementAnalyzer.addIndex()
        }
        toUpdate = updates
        whereClause = createWhereClause(statementAnalyzer, listOf(table),toUpdate.size+1)
    }

    override fun setSomething(parameterIndex: Int, x: Any?) {
        if (parameterIndex-1 < toUpdate.size) {
            val updatedValue = toUpdate[parameterIndex-1].column.columnType.validateValue(x)
            toUpdate[parameterIndex-1].value = updatedValue
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
                    .firstOrNull { it.column == cell.column}
                    ?.let { Cell(it.column,it.value)}?:cell)
            }
            newTable.addRow(Row(newCells))
        }
        dbTransaction.registerTableUpdate(newTable)
        return hits
    }
}