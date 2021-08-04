package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*
import no.anksoft.pginmem.values.ArrayCellValue
import no.anksoft.pginmem.values.CellValue
import java.sql.SQLException

class AnyClause(
    private val leftValueFromExpression: ValueFromExpression,
    indexToUse:IndexToUse,
    private val dbTransaction: DbTransaction,
):WhereClause {
    private val expectedIndex:Int = indexToUse.takeInd()
    private var expectedValue:ArrayCellValue? = null

    override fun isMatch(cells: List<Cell>): Boolean {
        val leftValue = leftValueFromExpression.genereateValue(dbTransaction,Row(cells))
        val expected = expectedValue?:throw SQLException("No binding set in any")
        val res = expected.myValues.any { it == leftValue }
        return res
    }

    override fun registerBinding(index: Int, value: CellValue): Boolean {
        if (index != expectedIndex) {
            return false
        }
        if (value !is ArrayCellValue) {
            throw SQLException("Only array accepted as bindings in any")
        }
        expectedValue = value
        return true
    }
}