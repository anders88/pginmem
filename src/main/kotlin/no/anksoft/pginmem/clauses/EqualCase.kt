package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*
import no.anksoft.pginmem.values.CellValue

class EqualCase(
    leftValueFromExpression: ValueFromExpression,
    expectedIndex: IndexToUse,
    statementAnalyzer: StatementAnalyzer,
    dbTransaction: DbTransaction,
    tables: Map<String, Table>
) : BinaryClause(leftValueFromExpression, expectedIndex, statementAnalyzer, dbTransaction, tables) {
    override fun matchValues(left: CellValue, right: CellValue): Boolean = (left == right)


}