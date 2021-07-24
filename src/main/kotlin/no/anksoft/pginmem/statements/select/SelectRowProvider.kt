package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.*
import no.anksoft.pginmem.clauses.WhereClause
import no.anksoft.pginmem.statements.OrderPart
import no.anksoft.pginmem.values.NullCellValue
import java.sql.SQLException
import java.util.*

interface SelectRowProvider {
    fun size():Int
    fun readRow(rowno: Int):Row
    val limit:Int?
    val offset:Int

    fun providerWithFixed(row:Row?):SelectRowProvider
}


private class TableJoinRow(val rowids:Map<String,String>,val cells:List<Cell>) {
    override fun equals(other: Any?): Boolean {
        if (other !is TableJoinRow) {
            return false
        }
        return ((cells == other.cells) && (rowids == other.rowids))
    }

    override fun hashCode(): Int = 1
}

class LeftOuterJoin(val leftTable:Table, val rightTable:Table, val leftcol:Column, val rightcol:Column)

private interface RowForSelect {
    fun reset()
    fun next():Row?

    fun isEmpty():Boolean
}

private class SimpleRowForSelect(tableInSelect:TableInSelect):RowForSelect {
    private val myrows = tableInSelect.rowsFromSelect()
    private var index = -1
    override fun reset() {
        index = -1
    }

    override fun next(): Row? {
        index++
        return if (index < myrows.size) myrows[index] else null
    }

    override fun isEmpty(): Boolean = myrows.isEmpty()
}

private class LeftOuterJoinRowForSelect(private val leftOuterJoin: LeftOuterJoin):RowForSelect {
    private val leftSelect:RowForSelect = SimpleRowForSelect(leftOuterJoin.leftTable)
    private val rigtSelect:RowForSelect = SimpleRowForSelect(leftOuterJoin.rightTable)
    private var currentLeftRow:Row? = null
    private var hasDeliveredFromCurrentLeft:Boolean = false

    override fun reset() {
        leftSelect.reset()
        rigtSelect.reset()
        currentLeftRow = null
        hasDeliveredFromCurrentLeft = false
    }

    override fun next(): Row? {
        if (currentLeftRow == null) {
            currentLeftRow = leftSelect.next()?:return null
        }
        while (true) {
            val rightRow:Row? = rigtSelect.next()
            if (rightRow == null) {
                if (hasDeliveredFromCurrentLeft) {
                    currentLeftRow = leftSelect.next()?:return null
                    rigtSelect.reset()
                    hasDeliveredFromCurrentLeft = false
                    continue
                }
                val resCells:MutableList<Cell> = mutableListOf()
                resCells.addAll(currentLeftRow!!.cells)
                for (column in leftOuterJoin.rightTable.colums) {
                    resCells.add(Cell(column,NullCellValue))
                }
                val rowIds:MutableMap<String,String> = mutableMapOf()
                rowIds.putAll(currentLeftRow!!.rowids)
                rowIds.put(leftOuterJoin.rightTable.name,UUID.randomUUID().toString())
                currentLeftRow = null
                return Row(resCells,rowIds)
            }
            val leftValue = currentLeftRow!!.cells.first { it.column.matches(leftOuterJoin.leftcol.tablename,leftOuterJoin.leftcol.name) }.value
            val rightValue = rightRow.cells.first { it.column.matches(leftOuterJoin.rightcol.tablename,leftOuterJoin.rightcol.name) }.value

            if (leftValue != rightValue) {
                continue
            }

            hasDeliveredFromCurrentLeft = true
            val resRow = Row(currentLeftRow!!.cells + rightRow.cells,currentLeftRow!!.rowids + rightRow.rowids)
            return resRow
        }

    }

    override fun isEmpty(): Boolean = leftSelect.isEmpty()

}



class TablesSelectRowProvider constructor(
    private val dbTransaction: DbTransaction,
    private val tablesPicked: List<TableInSelect>,
    private val whereClause: WhereClause,
    private val orderParts: List<OrderPart>,
    override val limit:Int?,
    override val offset: Int,
    private val injectCells:List<Cell> = emptyList(),
    private val leftOuterJoins:List<LeftOuterJoin> = emptyList()
    ):SelectRowProvider {


    private val values:List<TableJoinRow> by lazy {
        val tablesToParse:List<RowForSelect> = tablesPicked.map { t ->
            val leftJoin:LeftOuterJoin? = leftOuterJoins.firstOrNull { it.leftTable.name == t.name }
            val rightJoin:LeftOuterJoin? = leftOuterJoins.firstOrNull { it.rightTable.name == t.name }

            when {
                leftJoin != null -> LeftOuterJoinRowForSelect(leftJoin)
                rightJoin != null -> null
                else -> SimpleRowForSelect(t)
            }
        }.filterNotNull()

        if (tablesToParse.isEmpty() || tablesToParse.any { it.isEmpty() }) {
            emptyList()
        } else {
            val currentRowPicks:MutableList<Row?> = mutableListOf()
            for (i in 0 until tablesToParse.size-1) {
                currentRowPicks.add(tablesToParse[i].next())
            }
            currentRowPicks.add(null)

            val resultRows:MutableList<TableJoinRow> = mutableListOf()
            while (true) {
                var didInc = false
                var pos = tablesToParse.size-1
                while (pos >= 0) {
                    val r:Row? = tablesToParse[pos].next()
                    currentRowPicks[pos] = r
                    if (r != null) {
                        didInc = true
                        break
                    }
                    pos--
                }
                if (!didInc) {
                    break
                }
                for (resetpos in pos+1 until tablesToParse.size) {
                    tablesToParse[resetpos].reset()
                    val next = tablesToParse[resetpos].next()
                    currentRowPicks[resetpos] = next
                }
                val cellsThisRow: MutableList<Cell> = mutableListOf()
                val rowidsThisRow:MutableMap<String,String> = mutableMapOf()

                for (row in currentRowPicks) {
                    cellsThisRow.addAll(row!!.cells)
                    rowidsThisRow.putAll(row.rowids)
                }
                cellsThisRow.addAll(injectCells)

                if (whereClause.isMatch(cellsThisRow)) {
                    resultRows.add(TableJoinRow(rowidsThisRow,cellsThisRow))
                }

            }
            if (orderParts.isNotEmpty()) {
                resultRows.sortWith { a, b -> compareRows(a.cells,b.cells) }
            }
            resultRows
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

    override fun providerWithFixed(row: Row?): SelectRowProvider {
        if (row == null) {
            return this
        }
        return TablesSelectRowProvider(
            this.dbTransaction,
            this.tablesPicked,
            this.whereClause,
            this.orderParts,
            this.limit,
            this.offset,
            row.cells,
            this.leftOuterJoins
        )
    }
}

class ImplicitOneRowSelectProvider(val whereClause: WhereClause,val injectedRow:Row?=null):SelectRowProvider {
    private val rowExsists:Boolean by lazy {
        whereClause.isMatch(injectedRow?.cells?: emptyList())
    }
    override fun size(): Int = if (rowExsists) 1 else 0

    override fun readRow(rowno: Int): Row {
        if (!rowExsists) {
            throw SQLException("No row found")
        }
        return injectedRow?:Row(emptyList())
    }

    override val limit: Int? = null
    override val offset: Int = 0
    override fun providerWithFixed(row: Row?): SelectRowProvider = ImplicitOneRowSelectProvider(whereClause,row)

}

