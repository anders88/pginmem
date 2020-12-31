package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import java.sql.SQLException
import java.sql.Timestamp

private class LinkedValue(val column: Column,val index:Int?,var value:(Pair<DbTransaction,Row?>)->Any?={null})

class InsertIntoStatement constructor(statementAnalyzer: StatementAnalyzer, val dbTransaction: DbTransaction,private val sql:String) : StatementWithSet() {
    private val tableForUpdate:Table = dbTransaction.tableForUpdate(statementAnalyzer.word(2)?:throw SQLException("Expected table name"))
    private val columns:List<Column>
    private val linkedValues:List<LinkedValue>

    init {
        val cols:MutableList<Column> = mutableListOf()
        statementAnalyzer.addIndex(4)
        while (true) {
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
        columns = cols
        if (statementAnalyzer.addIndex().word() != "values") {
            throw SQLException("Expected values")
        }
        if (statementAnalyzer.addIndex().word() != "(") {
            throw SQLException("Expected (")
        }
        val linkedValues:MutableList<LinkedValue> = mutableListOf()
        var linkedIndex = 0
        for (i in columns.indices) {
            val insertcolval = statementAnalyzer.addIndex().word()
            val linkedValue:LinkedValue = if (insertcolval == "?") {
                linkedIndex++
                LinkedValue(columns[i],linkedIndex)
            } else {
                val value = statementAnalyzer.readValueFromExpression(dbTransaction, listOf(tableForUpdate))?:throw SQLException("Could not read value in statement")
                LinkedValue(columns[i],null, value)
            }
            linkedValues.add(linkedValue)
            val nextexp = if (i == columns.size-1) ")" else ","
            if (statementAnalyzer.addIndex().word() != nextexp) {
                throw SQLException("Expected $nextexp")
            }
        }
        this.linkedValues = linkedValues
    }

    override fun setSomething(parameterIndex: Int, x: Any?) {
        val linkedValue:LinkedValue = linkedValues.firstOrNull { it.index == parameterIndex }?:throw SQLException("Unknown binding index $parameterIndex")
        val setVal = linkedValue.column.columnType.validateValue(x)
        val genvalue:(Pair<DbTransaction,Row?>)->Any? = {setVal}
        linkedValue.value= genvalue
    }




    override fun executeUpdate(): Int {
        val cells:List<Cell> = tableForUpdate.colums.map{ col ->
            val index = columns.indexOfFirst { it.name == col.name }
            val value:Any? = if (index == -1) {
                if (col.defaultValue != null) {
                    col.defaultValue.invoke(Pair(dbTransaction,null))
                } else null
            } else linkedValues[index].value.invoke(Pair(dbTransaction,null))
            if (col.isNotNull && value == null) {
                throw SQLException("Cannot insert null into column ${col.name}")
            }
            Cell(col,value)
        }
        tableForUpdate.addRow(Row(cells))
        dbTransaction.registerTableUpdate(tableForUpdate)
        return 1
    }
}