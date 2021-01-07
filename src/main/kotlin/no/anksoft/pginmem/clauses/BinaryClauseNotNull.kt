package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*
import java.sql.SQLException

abstract class BinaryClauseNotNull:BinaryClause {
    constructor(leftValueFromExpression: ValueFromExpression, expectedIndex:IndexToUse,statementAnalyzer: StatementAnalyzer,dbTransaction: DbTransaction,tables:Map<String,Table>):super(leftValueFromExpression,expectedIndex,statementAnalyzer,dbTransaction,tables)

    override fun matchValues(left:Any?,right:Any?):Boolean {
        if (left !is Comparable<*>) return false
        if (right == null) return false
        return checkMatch(left,right)
    }

    abstract fun <T> checkMatch(first: Comparable<T>, second: Any?): Boolean
}