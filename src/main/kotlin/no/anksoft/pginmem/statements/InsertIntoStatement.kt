package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import java.sql.SQLException
import java.sql.Timestamp

private class LinkedValue(var value:Any?)

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
        linkedValues = (1..columns.size).map { LinkedValue(null) }
    }

    override fun setSomething(parameterIndex: Int, x: Any?) {
        val setVal = columns[parameterIndex-1].columnType.validateValue(x)
        linkedValues[parameterIndex-1].value=setVal
    }




    override fun executeUpdate(): Int {
        val cells:List<Cell> = tableForUpdate.colums.map{ col ->
            val index = columns.indexOfFirst { it.name == col.name }
            val value:Any? = if (index == -1) {
                if (col.defaultValue != null) {
                    col.defaultValue.invoke(dbTransaction)
                } else null
            } else linkedValues[index].value
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