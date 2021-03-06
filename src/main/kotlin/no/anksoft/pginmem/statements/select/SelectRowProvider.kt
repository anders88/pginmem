package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.Cell
import no.anksoft.pginmem.Column
import no.anksoft.pginmem.Row
import no.anksoft.pginmem.Table
import no.anksoft.pginmem.clauses.WhereClause
import no.anksoft.pginmem.statements.OrderPart
import no.anksoft.pginmem.values.CellValue
import java.sql.SQLException

interface SelectRowProvider {
    fun size():Int
    fun readRow(rowno: Int):Row
    val limit:Int?
    val offset:Int
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

private class TableJoinRow(val rowids:Map<String,String>,val cells:List<Cell>)

class TablesSelectRowProvider constructor(private val tables: List<Table>, private val whereClause: WhereClause, private val orderParts: List<OrderPart>, override val limit:Int?,
                                          override val offset: Int):SelectRowProvider {

    private val values:List<TableJoinRow> by lazy {
        if (tables.isEmpty() || tables.any { it.size() <= 0 }) {
            emptyList()
        } else {
            val indexes: MutableList<Int> = mutableListOf()
            for (i in 1..tables.size) {
                indexes.add(0)
            }

            val genrows: MutableList<TableJoinRow> = mutableListOf()

            do {
                val cellsThisRow: MutableList<Cell> = mutableListOf()
                val rowidsThisRow:MutableMap<String,String> = mutableMapOf()
                for (i in 0 until indexes.size) {
                    val row:Row = tables[i].rowsForReading()[indexes[i]]
                    val tc: List<Cell> = row.cells
                    cellsThisRow.addAll(tc)
                    rowidsThisRow.putAll(row.rowids)
                }
                if (whereClause.isMatch(cellsThisRow)) {
                    genrows.add(TableJoinRow(rowidsThisRow,cellsThisRow))
                }
                val isMore = incIndex(indexes, tables)
            } while (isMore)

            if (orderParts.isNotEmpty()) {
                genrows.sortWith { a, b -> compareRows(a.cells,b.cells) }
            }
            genrows
        }
    }

    private fun compareRows(a:List<Cell>,b:List<Cell>):Int {
        for (orderPart in orderParts) {
            val aVal = a.first { it.column == orderPart.column }.value
            val bVal = b.first { it.column == orderPart.column}.value
            if (aVal == bVal) {
                continue
            }
            return if (orderPart.ascending) aVal.compareMeTo(bVal,orderPart.nullsFirst) else bVal.compareMeTo(aVal,orderPart.nullsFirst)
        }
        return 0
    }


    override fun size(): Int = values.size

    override fun readRow(rowno: Int): Row {
        val myRow:TableJoinRow = values[rowno]
        return Row(myRow.cells,myRow.rowids)
    }
}

class ImplicitOneRowSelectProvider:SelectRowProvider {
    override fun size(): Int = 1

    override fun readRow(rowno: Int): Row {
        return Row(emptyList())
    }

    override val limit: Int? = null
    override val offset: Int = 0

}

class AggregateSelectRowProvider:SelectRowProvider {
    override fun size(): Int {
        TODO("Not yet implemented")
    }


    override fun readRow(rowno: Int): Row {
        TODO("Not yet implemented")
    }

    override val limit: Int? = null

    override val offset: Int = 0
}