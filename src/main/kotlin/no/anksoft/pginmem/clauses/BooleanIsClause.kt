package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*
import no.anksoft.pginmem.values.BooleanCellValue
import no.anksoft.pginmem.values.CellValue

class BooleanIsClause(
    private val leftValueFromExpression: ValueFromExpression,
    private val dbTransaction: DbTransaction,
    private val expBoolean:Boolean,
    private val notFlag:Boolean
):WhereClause {
    override fun isMatch(cells: List<Cell>): Boolean {
        val leftValue = leftValueFromExpression.genereateValue(dbTransaction, Row(cells))
        if (!notFlag) {
            return (BooleanCellValue(expBoolean) == leftValue)
        }
        return (BooleanCellValue(expBoolean) != leftValue)
    }

    override fun registerBinding(index: Int, value: CellValue): Boolean = false
}