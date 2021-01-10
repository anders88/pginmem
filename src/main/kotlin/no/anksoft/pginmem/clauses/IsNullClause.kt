package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*
import no.anksoft.pginmem.values.CellValue
import no.anksoft.pginmem.values.NullCellValue

class IsNullClause(val dbTransaction: DbTransaction,val leftValueFromExpression: ValueFromExpression):WhereClause {
    override fun isMatch(cells: List<Cell>): Boolean {
        val value = leftValueFromExpression.valuegen.invoke(Pair(dbTransaction,Row(cells)))
        return (value == NullCellValue)
    }

    override fun registerBinding(index: Int, value: CellValue): Boolean = false
}