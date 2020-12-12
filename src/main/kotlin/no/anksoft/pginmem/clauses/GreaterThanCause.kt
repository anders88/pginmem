package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.Column

@Suppress("UNCHECKED_CAST")
class GreaterThanCause: BinaryClause {
    constructor(column: Column,expectedIndex:Int):super(column,expectedIndex)
    constructor(column: Column,valueToMatch:Any?):super(column,valueToMatch)


    override fun <T> checkMatch(first: Comparable<T>, second: Any?): Boolean {
        if (second == null) {
            return false
        }
        return first > second as T
    }


}