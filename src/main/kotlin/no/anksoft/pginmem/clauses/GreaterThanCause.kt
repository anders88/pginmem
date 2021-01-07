package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*

@Suppress("UNCHECKED_CAST")
class GreaterThanCause(
    leftValueFromExpression: ValueFromExpression,
    expectedIndex: IndexToUse,
    statementAnalyzer: StatementAnalyzer,
    dbTransaction: DbTransaction,
    tables: Map<String, Table>
) : BinaryClauseNotNull(leftValueFromExpression, expectedIndex, statementAnalyzer, dbTransaction, tables) {


    override fun <T> checkMatch(first: Comparable<T>, second: Any?): Boolean {
        if (second == null) {
            return false
        }
        return first > second as T
    }


}