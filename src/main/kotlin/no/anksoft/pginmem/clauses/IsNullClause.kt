package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*

class IsNullClause(val dbTransaction: DbTransaction,val leftValueFromExpression: ValueFromExpression):WhereClause {
    override fun isMatch(cells: List<Cell>): Boolean {
        val value = leftValueFromExpression.valuegen.invoke(Pair(dbTransaction,Row(cells)))
        return (value == null)
    }

    override fun registerBinding(index: Int, value: Any?): Boolean = false
}