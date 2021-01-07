package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*

class NotEqualCause(
    leftValueFromExpression: ValueFromExpression,
    expectedIndex: IndexToUse,
    statementAnalyzer: StatementAnalyzer,
    dbTransaction: DbTransaction,
    tables: Map<String, Table>
) : BinaryClause(leftValueFromExpression, expectedIndex, statementAnalyzer, dbTransaction, tables) {

    override fun matchValues(left: Any?, right: Any?): Boolean {
        return (left != right)
    }

}