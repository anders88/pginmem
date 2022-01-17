package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*
import no.anksoft.pginmem.statements.select.TableInSelect
import no.anksoft.pginmem.values.CellValue
import no.anksoft.pginmem.values.NullCellValue


class LikeClause(
    leftValueFromExpression: ValueFromExpression,
    expectedIndex: IndexToUse,
    statementAnalyzer: StatementAnalyzer,
    dbTransaction: DbTransaction,
    tables: Map<String, TableInSelect>
) : BinaryClause(leftValueFromExpression, expectedIndex, statementAnalyzer, dbTransaction, tables) {

    override fun matchValues(left: CellValue, right: CellValue): Boolean {
        if (left == NullCellValue || right == NullCellValue) {
            return false
        }
        val leftval = left.valueAsText().myValue
        val rightval = right.valueAsText().myValue

        return when {
            rightval.startsWith("%") && rightval.endsWith("%") && rightval.length >= 3 -> leftval.contains(rightval.substring(1,rightval.length-1))
            rightval.startsWith("%") && rightval.length >= 2 -> leftval.endsWith(rightval.substring(1))
            rightval.endsWith("%") && rightval.length >= 2 -> leftval.startsWith(rightval.substring(0,rightval.length-1))
            else -> (leftval == rightval)
        }

    }


}