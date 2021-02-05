package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import no.anksoft.pginmem.clauses.WhereClause
import no.anksoft.pginmem.clauses.createWhereClause
import no.anksoft.pginmem.statements.select.SelectResultSet
import no.anksoft.pginmem.values.CellValue
import no.anksoft.pginmem.values.NullCellValue
import java.sql.SQLException

private fun <T> wordvalue(list:List<T>, index:Int):T = if (index >= 0 && index < list.size) list[index] else throw SQLException("Unexpected end of statement")


private class CellToUpdateByBinding(val column:Column) {
    var value:CellValue=NullCellValue
}

private class CellToUpdateByFunction(val column: Column,val valueFromExpression: ValueFromExpression)

class UpdateStatement(statementAnalyzer: StatementAnalyzer, private val dbTransaction: DbTransaction) : StatementWithSet() {
    private val table:Table
    //private val whereClause: WhereClause
    private val toUpdateByBinding:List<CellToUpdateByBinding>
    private val toUpdateByFunction:List<CellToUpdateByFunction>

    private val selectStatement:SelectStatement

    private val numBindingsBeforeWhere:Int


    init {
        table = dbTransaction.tableForUpdate(statementAnalyzer.addIndex(1).word()?:throw SQLException("Unexpected end of statement") )

        var numBindingsBeforeWhere:Int = 0
        var i = 0
        while ((statementAnalyzer.word(i)?:"where") != "where") {
            if (statementAnalyzer.word(i) == "?") {
                numBindingsBeforeWhere++
            }
            i++
        }
        this.numBindingsBeforeWhere = numBindingsBeforeWhere

        val toPrepend:List<String> = listOf("select","*","from",table.name,table.name)

        val selectStatementAnalyzer = statementAnalyzer.extractSelect(toPrepend)
        selectStatement = SelectStatement(selectStatementAnalyzer,dbTransaction,numBindingsBeforeWhere+1)

        val tableAliasesMap:MutableMap<String,String> = mutableMapOf()
        selectStatement.selectAnalyze.selectedColumns.forEach { tableAliasesMap.putAll(it.tableAliases) }

        val tableAliases:Map<String,Table> = tableAliasesMap.entries.associate { it.key to dbTransaction.tableForRead(it.value) }

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
                val function = statementAnalyzer.readValueFromExpression(dbTransaction,tableAliases)
                statementAnalyzer.addIndex()
                toUpdateByFunctions.add(CellToUpdateByFunction(column,function))
            }
            if (setOf("where","from").contains(statementAnalyzer.word()?:"where")) {
                break
            }
            if (statementAnalyzer.word() != ",") {
                throw SQLException("Expected where or ,")
            }
            statementAnalyzer.addIndex()
        }
        toUpdateByBinding = updateByBindings
        toUpdateByFunction = toUpdateByFunctions
        //whereClause = createWhereClause(statementAnalyzer, mapOf(Pair(table.name,table)),toUpdateByBinding.size+1,dbTransaction)
    }

    override fun setSomething(parameterIndex: Int, x: CellValue) {
        if (parameterIndex > numBindingsBeforeWhere) {
            selectStatement.setSomething(parameterIndex,x)
            return
        }
            val updatedValue = toUpdateByBinding[parameterIndex-1].column.columnType.validateValue(x)
            toUpdateByBinding[parameterIndex-1].value = updatedValue
            return
    }


    override fun executeUpdate(): Int {
        val newTable = Table(table.name,table.colums)

        val selectResultSet:SelectResultSet = selectStatement.internalExecuteQuery()


        for (exsistingRow:Row in table.rowsForReading()) {
            var matchedUpdateRow:Int? = null
            for (i in 0 until selectResultSet.numberOfRows) {
                val possibleRow = selectResultSet.selectRowProviderGiven.readRow(i)
                val possibleRowid = possibleRow.rowids[table.name]
                if (possibleRowid != null && exsistingRow.rowids[table.name] == possibleRowid) {
                    matchedUpdateRow = i
                    break
                }
            }
            if (matchedUpdateRow == null) {
                newTable.addRow(exsistingRow)
                continue
            }
            val rowCells:List<Cell> = table.colums.map { column ->
                val colvalue:CellValue = toUpdateByBinding.firstOrNull { it.column == column }?.value
                    ?: toUpdateByFunction.firstOrNull { it.column == column }
                        ?.valueFromExpression?.valuegen?.invoke(Pair(dbTransaction,selectResultSet.selectRowProviderGiven.readRow(matchedUpdateRow)))
                    ?: selectResultSet.valueAt(table.name + "." + column.name,matchedUpdateRow)
                Cell(column,colvalue)
            }
            newTable.addRow(Row(rowCells,exsistingRow.rowids))
        }
        
        dbTransaction.registerTableUpdate(newTable)
        return selectResultSet.numberOfRows
    }
}