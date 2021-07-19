package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.*
import no.anksoft.pginmem.statements.SelectAnalyze
import java.util.*

class SelectAsATable(override val name: String,private val selectAnalyze: SelectAnalyze,private val dbTransaction: DbTransaction):TableInSelect {


    private val myRows:List<Row> by lazy {
        val columns:List<Column> = selectAnalyze.selectedColumns.map {
            Column.create(it,name)
        }
        val selectResultSet = SelectResultSet(
            selectAnalyze.selectedColumns,
            selectAnalyze.selectRowProvider,
            dbTransaction,
            selectAnalyze.distinctFlag
        )

        val res:MutableList<Row> = mutableListOf()
        for (rowno in 0 until selectResultSet.numberOfRows) {
            val cells:MutableList<Cell> = mutableListOf()
            for (colno in columns.indices) {
                val value = selectResultSet.valueAt(colno + 1, rowno)
                cells.add(Cell(columns[colno],value))
            }
            val arow = Row(cells, mapOf(Pair(name,UUID.randomUUID().toString())))
            res.add(arow)
        }
        res
    }

    override val colums: List<ColumnInSelect> = selectAnalyze.selectedColumns.map { scp ->
        SelectColumnAsAColumn(scp,name)
    }

    override fun findColumn(colname: String): ColumnInSelect? {
        val actualName = stripSeachName(colname)
        return colums.firstOrNull { it.matches(this.name,actualName)}
    }

    override fun rowsFromSelect(): List<Row> = myRows

    override fun size(): Int = myRows.size

}