package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import no.anksoft.pginmem.statements.select.SelectResultSet
import no.anksoft.pginmem.values.CellValue
import no.anksoft.pginmem.values.NullCellValue
import java.sql.SQLException
import java.util.*

private class LinkedValue(val column: Column,val index:Int?,var value:(Pair<DbTransaction,Row?>)->CellValue={NullCellValue})

class InsertIntoStatement constructor(statementAnalyzer: StatementAnalyzer, val dbTransaction: DbTransaction,private val sql:String) : StatementWithSet(dbTransaction) {
    private val tableForUpdate:Table = dbTransaction.tableForUpdate(statementAnalyzer.word(2)?:throw SQLException("Expected table name"))
    private val columns:List<Column>
    private val linkedValues:List<LinkedValue>
    private val selectStatement:SelectStatement?

    init {
        val cols:MutableList<Column> = mutableListOf()
        statementAnalyzer.addIndex(3)
        val readCols:Boolean = if (statementAnalyzer.word() == "(") {
            statementAnalyzer.addIndex(1)
            true
        } else false

        while (readCols) {
            cols.add(statementAnalyzer.word()?.let {  tableForUpdate.findColumn(it)}?:throw SQLException("Unknown column ${statementAnalyzer.word()}"))
            statementAnalyzer.addIndex()
            if (statementAnalyzer.word() == ")") {
                break
            }
            if (statementAnalyzer.word() != ",") {
                throw SQLException("Expected , or ) got ${statementAnalyzer.word()}")
            }
            statementAnalyzer.addIndex()
        }
        columns = if (readCols) cols else tableForUpdate.colums
        if (statementAnalyzer.addIndex(if (readCols) 1 else 0).word() == "values") {
            this.linkedValues = readLinkedValues(statementAnalyzer)
            this.selectStatement = null
        } else {
            this.linkedValues = emptyList()
            if (statementAnalyzer.word() == "(") {
                statementAnalyzer.addIndex()
            }
            if (statementAnalyzer.word() != "select") {
                throw SQLException("Expected values or select in insert statament")
            }
            this.selectStatement = SelectStatement(statementAnalyzer,dbTransaction)
        }

    }

    private fun readLinkedValues(statementAnalyzer: StatementAnalyzer): List<LinkedValue> {
        if (statementAnalyzer.addIndex().word() != "(") {
            throw SQLException("Expected (")
        }
        val linkedValues: MutableList<LinkedValue> = mutableListOf()
        var linkedIndex = 0
        for (i in columns.indices) {
            val insertcolval = statementAnalyzer.addIndex().word()
            val linkedValue: LinkedValue = if (insertcolval == "?") {
                linkedIndex++
                LinkedValue(columns[i], linkedIndex)
            } else {
                val value = statementAnalyzer.readValueFromExpression(
                    dbTransaction,
                    mapOf(Pair(tableForUpdate.name, tableForUpdate)),
                    null
                )
                    ?: throw SQLException("Could not read value in statement")
                LinkedValue(columns[i], null, value.valuegen)
            }
            linkedValues.add(linkedValue)
            val nextexp = if (i == columns.size - 1) ")" else ","
            if (statementAnalyzer.addIndex().word() != nextexp) {
                throw SQLException("Expected $nextexp")
            }
        }
        return linkedValues
    }

    override fun setSomething(parameterIndex: Int, x: CellValue) {
        if (selectStatement != null) {
            selectStatement.registerBinding(parameterIndex, x)
            return
        }
        val linkedValue:LinkedValue = linkedValues.firstOrNull { it.index == parameterIndex }?:throw SQLException("Unknown binding index $parameterIndex")
        val setVal = linkedValue.column.columnType.validateValue(x)
        val genvalue:(Pair<DbTransaction,Row?>)->CellValue = {setVal}
        linkedValue.value= genvalue
    }




    override fun executeUpdate(): Int {
        if (selectStatement == null) {
            val cells: List<Cell> = tableForUpdate.colums.map { col ->
                val index = columns.indexOfFirst { it == col }
                val value: CellValue = if (index == -1) {
                    if (col.defaultValue != null) {
                        col.defaultValue.invoke(Pair(dbTransaction, null))
                    } else NullCellValue
                } else linkedValues[index].value.invoke(Pair(dbTransaction, null))
                if (col.isNotNull && value == NullCellValue) {
                    throw SQLException("Cannot insert null into column $col")
                }
                Cell(col, value)
            }
            tableForUpdate.addRow(Row(cells, mapOf(Pair(tableForUpdate.name,UUID.randomUUID().toString()))))
            dbTransaction.registerTableUpdate(tableForUpdate)
            return 1
        }
        val selectResultSet:SelectResultSet = selectStatement.internalExecuteQuery()
        val numRows = selectResultSet.numberOfRows
        for (rowno in 0 until numRows) {
            val cells: List<Cell> = tableForUpdate.colums.map { col ->
                val index = columns.indexOfFirst { it == col }
                val value: CellValue = if (index == -1) {
                    if (col.defaultValue != null) {
                        col.defaultValue.invoke(Pair(dbTransaction, null))
                    } else NullCellValue
                } else selectResultSet.valueAt(index+1,rowno)
                if (col.isNotNull && value == NullCellValue) {
                    throw SQLException("Cannot insert null into column $col")
                }
                Cell(col, value)
            }
            tableForUpdate.addRow(Row(cells, mapOf(Pair(tableForUpdate.name,UUID.randomUUID().toString()))))
        }
        dbTransaction.registerTableUpdate(tableForUpdate)
        return numRows
    }
}