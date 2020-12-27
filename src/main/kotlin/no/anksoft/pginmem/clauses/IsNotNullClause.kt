package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.Cell
import no.anksoft.pginmem.Column

class IsNotNullClause(val column: Column):WhereClause {
    override fun isMatch(cells: List<Cell>): Boolean {
        val cell: Cell = cells.firstOrNull { it.column == column }?:return false
        return (cell.value != null)
    }

    override fun registerBinding(index: Int, value: Any?): Boolean = false
}