package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import no.anksoft.pginmem.clauses.WhereClause
import no.anksoft.pginmem.clauses.createWhereClause
import java.sql.SQLException

private fun <T> wordvalue(list:List<T>, index:Int):T = if (index >= 0 && index < list.size) list[index] else throw SQLException("Unexpected end of statement")


private class CellToUpdateByBinding(val column:Column) {
    var value:Any?=null
}

private class CellToUpdateByFunction(val column: Column,val function:(Row)->Any?)

class UpdateStatement(statementAnalyzer: StatementAnalyzer, private val dbTransaction: DbTransaction) : StatementWithSet() {
    private val table:Table
    private val whereClause: WhereClause
    private val toUpdateByBinding:List<CellToUpdateByBinding>
    private val toUpdateByFunction:List<CellToUpdateByFunction>

    init {

        table = dbTransaction.tableForUpdate(statementAnalyzer.addIndex(1).word()?:throw SQLException("Unexpected end of statement") )
        statementAnalyzer.addIndex(2)
        val updateByBindings:MutableList<CellToUpdateByBinding> = mutableListOf()
        val toUpdateByFunctions:MutableList<CellToUpdateByFunction> = mutableListOf()
        while (true) {
            val colnameToUpdate = statementAnalyzer.word() ?: throw SQLException("Unexpected end of statement")
            val column:Column = table.findColumn(colnameToUpdate)?:throw SQLException("Unknown column ${statementAnalyzer.word()}")
            if (statementAnalyzer.addIndex().word() != "=") {
                throw SQLException("Expected =")
            }
            if (statementAnalyzer.addIndex().word() == "?") {
                updateByBindings.add(CellToUpdateByBinding(column))
                statementAnalyzer.addIndex()
            } else {
                val function = statementAnalyzer.readValueOnRow(listOf(table))
                toUpdateByFunctions.add(CellToUpdateByFunction(column,function))
            }
            if (statementAnalyzer.word()?:"where" == "where") {
                break
            }
            if (statementAnalyzer.word() != ",") {
                throw SQLException("Expected where or ,")
            }
            statementAnalyzer.addIndex()
        }
        toUpdateByBinding = updateByBindings
        toUpdateByFunction = toUpdateByFunctions
        whereClause = createWhereClause(statementAnalyzer, mapOf(Pair(table.name,table)),toUpdateByBinding.size+1,dbTransaction)
    }

    override fun setSomething(parameterIndex: Int, x: Any?) {
        if (parameterIndex-1 < toUpdateByBinding.size) {
            val updatedValue = toUpdateByBinding[parameterIndex-1].column.columnType.validateValue(x)
            toUpdateByBinding[parameterIndex-1].value = updatedValue
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
                val useCell:Cell =
                    toUpdateByBinding.firstOrNull { it.column == cell.column }
                    ?.let { Cell(it.column, it.value) }
                    ?: toUpdateByFunction.firstOrNull { it.column == cell.column }
                    ?.let { Cell(it.column,it.function.invoke(row)) }
                    ?: cell
                newCells.add(useCell)
            }
            newTable.addRow(Row(newCells))
        }
        dbTransaction.registerTableUpdate(newTable)
        return hits
    }
}