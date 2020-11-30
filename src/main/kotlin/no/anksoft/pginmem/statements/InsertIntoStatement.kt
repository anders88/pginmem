package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import java.sql.SQLException
import java.sql.Timestamp

private class LinkedValue(var value:Any?)

class InsertIntoStatement(words: List<String>, dbStore: DbStore) : DbPreparedStatement() {
    private val tableForUpdate:Table = dbStore.tableForUpdate(words[2])?:throw SQLException("Unkown table ${words[2]}")
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

    private fun bindValue(parameterIndex: Int,x: Any?) {
        columns[parameterIndex-1].columnType.validateValue(x)
        linkedValues[parameterIndex-1].value=x
    }

    override fun setString(parameterIndex: Int, x: String?) {
        bindValue(parameterIndex,x)
    }

    override fun setTimestamp(parameterIndex: Int, x: Timestamp?) {
        bindValue(parameterIndex,x)
    }



    override fun executeUpdate(): Int {
        val cells:List<Cell> = tableForUpdate.colums.mapIndexed{ index,col ->
            Cell(col,linkedValues[index].value)
        }
        tableForUpdate.addRow(Row(cells))
        return 1
    }
}