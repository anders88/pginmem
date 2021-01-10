package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*
import no.anksoft.pginmem.values.CellValue
import no.anksoft.pginmem.values.NullCellValue


class GreaterThanCause(
    leftValueFromExpression: ValueFromExpression,
    expectedIndex: IndexToUse,
    statementAnalyzer: StatementAnalyzer,
    dbTransaction: DbTransaction,
    tables: Map<String, Table>
) : BinaryClause(leftValueFromExpression, expectedIndex, statementAnalyzer, dbTransaction, tables) {

    override fun matchValues(left: CellValue, right: CellValue): Boolean {
        if (left == NullCellValue || right == NullCellValue) return false
        return (left.compareMeTo(right,false) > 0)
    }


}