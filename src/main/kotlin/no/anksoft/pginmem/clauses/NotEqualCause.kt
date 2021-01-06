package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.Column
import no.anksoft.pginmem.DbTransaction
import no.anksoft.pginmem.StatementAnalyzer
import no.anksoft.pginmem.Table

class NotEqualCause:BinaryClause {
    constructor(column: Column, expectedIndex:IndexToUse, statementAnalyzer: StatementAnalyzer, dbTransaction: DbTransaction, tables:Map<String, Table>):super(column,expectedIndex,statementAnalyzer,dbTransaction,tables)

    override fun matchValues(left: Any?, right: Any?): Boolean {
        return (left != right)
    }

}