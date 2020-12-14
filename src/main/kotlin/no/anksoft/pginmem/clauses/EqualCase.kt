package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.Column

class EqualCase:BinaryClause {

    constructor(column: Column,expectedIndex:Int):super(column,expectedIndex)

    constructor(column: Column,valueToMatch:Any?):super(column,valueToMatch)

    override fun <T> checkMatch(first: Comparable<T>, second: Any?): Boolean {
        return first == second
    }

}