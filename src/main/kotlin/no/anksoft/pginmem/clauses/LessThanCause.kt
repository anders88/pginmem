package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.Column
import no.anksoft.pginmem.DbTransaction
import no.anksoft.pginmem.StatementAnalyzer
import no.anksoft.pginmem.Table

@Suppress("UNCHECKED_CAST")
class LessThanCause: BinaryClause {
    constructor(column: Column, expectedIndex:IndexToUse, statementAnalyzer: StatementAnalyzer, dbTransaction: DbTransaction, tables:Map<String, Table>):super(column,expectedIndex,statementAnalyzer,dbTransaction,tables)


    override fun <T> checkMatch(first: Comparable<T>, second: Any?): Boolean {
        if (second == null) {
            return false
        }
        return first < second as T
    }


}