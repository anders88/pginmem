package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import java.sql.SQLException

private class LinkedValue(var value:String?)

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
        }
        columns = cols
        linkedValues = (1..columns.size).map { LinkedValue(null) }
    }

    override fun setString(parameterIndex: Int, x: String?) {
        linkedValues[parameterIndex-1].value=x
    }

    override fun executeUpdate(): Int {
        val cells:List<Cell> = tableForUpdate.colums.mapIndexed{ index,col ->
            Cell(col,linkedValues[index].value)
        }
        tableForUpdate.addRow(Row(cells))
        return 1
    }
}