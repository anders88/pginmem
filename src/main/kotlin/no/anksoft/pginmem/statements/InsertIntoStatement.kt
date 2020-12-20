package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import java.sql.SQLException
import java.sql.Timestamp

private class LinkedValue(var value:Any?)

class InsertIntoStatement constructor(words: List<String>, dbTransaction: DbTransaction,private val sql:String) : StatementWithSet() {
    private val tableForUpdate:Table = dbTransaction.tableForUpdate(words[2])
    private val columns:List<Column>
    private val linkedValues:List<LinkedValue>

    init {
        val cols:MutableList<Column> = mutableListOf()
        var ind = 4
        while (true) {
            cols.add(tableForUpdate.findColumn(words[ind])?:throw SQLException("Unknown column ${words[ind]}"))
            ind++
            if (words[ind] == ")") {
                break
            }
            if (words[ind] != ",") {
                throw SQLException("Expected , or ) got ${words[ind]}")
            }
            ind++
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
                    col.defaultValue.invoke()
                } else null
            } else linkedValues[index].value
            Cell(col,value)
        }
        tableForUpdate.addRow(Row(cells))
        return 1
    }
}