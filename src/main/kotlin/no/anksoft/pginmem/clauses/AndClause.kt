package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.Cell

class AndClause(private val left: WhereClause,private val right:WhereClause,private val bindLimit:Int):WhereClause {
    override fun isMatch(cells: List<Cell>): Boolean {
        val leftMatch = left.isMatch(cells)
        return leftMatch && right.isMatch(cells)
    }

    override fun registerBinding(index: Int, value: Any?): Boolean {
        if (index < bindLimit) {
            return left.registerBinding(index,value)
        } else {
            return right.registerBinding(index,value)
        }
    }
}