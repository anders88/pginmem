package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*
import no.anksoft.pginmem.statements.select.TableInSelect
import no.anksoft.pginmem.values.CellValue
import no.anksoft.pginmem.values.NullCellValue

@Suppress("UNCHECKED_CAST")
class LessThanOrEqualCause(
    leftValueFromExpression: ValueFromExpression,
    expectedIndex: IndexToUse,
    statementAnalyzer: StatementAnalyzer,
    dbTransaction: DbTransaction,
    tables: Map<String, TableInSelect>
) : BinaryClause(leftValueFromExpression, expectedIndex, statementAnalyzer, dbTransaction, tables) {

    override fun matchValues(left: CellValue, right: CellValue): Boolean {
        if (left == NullCellValue || right == NullCellValue) return false
        return (left.compareMeTo(right) <= 0)
    }


}