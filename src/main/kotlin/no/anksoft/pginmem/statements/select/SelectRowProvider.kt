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

class TablesSelectRowProvider(val table: Table,val whereClause: WhereClause):SelectRowProvider {

    private val values:List<List<Cell>> by lazy {
        table.rowsForReading().filter { whereClause.isMatch(it.cells) }.map { it.cells }
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