package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*

class EqualCase(
    leftValueFromExpression: ValueFromExpression,
    expectedIndex: IndexToUse,
    statementAnalyzer: StatementAnalyzer,
    dbTransaction: DbTransaction,
    tables: Map<String, Table>
) : BinaryClauseNotNull(leftValueFromExpression, expectedIndex, statementAnalyzer, dbTransaction, tables) {


    override fun <T> checkMatch(first: Comparable<T>, second: Any?): Boolean {
        return first == second
    }

}