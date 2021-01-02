package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.Cell
import no.anksoft.pginmem.Column
import no.anksoft.pginmem.Table
import no.anksoft.pginmem.clauses.WhereClause
import java.sql.SQLException

interface SelectRowProvider {
    fun size():Int
    fun readValue(column: Column,rowno: Int):Any?
}

private fun incIndex(indexes:MutableList<Int>,tables: List<Table>):Boolean {
    var currTable = tables.size - 1
    while (currTable >= 0) {
        if (indexes[currTable] < tables[currTable].size()-1) {
            indexes[currTable] = indexes[currTable]+1
            return true
        }
        indexes[currTable] = 0
        currTable--
    }
    return false
}

class TablesSelectRowProvider(private val tables: List<Table>,private val whereClause: WhereClause):SelectRowProvider {

    private val values:List<List<Cell>> by lazy {
        if (tables.isEmpty() || tables.any { it.size() <= 0 }) {
            emptyList()
        } else {
            val indexes: MutableList<Int> = mutableListOf()
            for (i in 1..tables.size) {
                indexes.add(0)
            }

            val genrows: MutableList<List<Cell>> = mutableListOf()

            do {
                val cellsThisRow: MutableList<Cell> = mutableListOf()
                for (i in 0 until indexes.size) {
                    val tc: List<Cell> = tables[i].rowsForReading()[indexes[i]].cells
                    cellsThisRow.addAll(tc)
                }
                if (whereClause.isMatch(cellsThisRow)) {
                    genrows.add(cellsThisRow)
                }
                val isMore = incIndex(indexes, tables)
            } while (isMore)

            genrows
            //table.rowsForReading().filter { whereClause.isMatch(it.cells) }.map { it.cells }
        }
    }

    override fun size(): Int = values.size

    override fun readValue(column: Column,rowno:Int): Any? {
        val row = values[rowno]
        val cell = row.first { it.column == column }
        return cell.value
    }
}

class ImplicitOneRowSelectProvider:SelectRowProvider {
    override fun size(): Int = 1

    override fun readValue(column: Column, rowno: Int): Any? {
        throw SQLException("No columns in select")
    }

}