package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.Cell
import no.anksoft.pginmem.Column
import java.sql.SQLException

class EqualCase:WhereClause {
    private val column: Column
    private val expectedIndex:Int?
    private var valueToMatch:Any?
    private var isRegistered:Boolean

    constructor(column: Column,expectedIndex:Int) {
        this.column = column
        this.valueToMatch = null
        this.expectedIndex = expectedIndex
        this.isRegistered = false
    }

    constructor(column: Column,valueToMatch:Any?) {
        this.column = column
        this.valueToMatch = valueToMatch
        this.expectedIndex = null
        this.isRegistered = true
    }

    override fun isMatch(cells: List<Cell>): Boolean {
        if (!isRegistered) {
            throw SQLException("Binding not set")
        }
        val cell:Cell = cells.firstOrNull { it.column.name == column.name }?:return false
        return (cell.value == valueToMatch)
    }

    override fun registerBinding(index: Int, value: Any?):Boolean {
        if (expectedIndex == index) {
            valueToMatch = value
            isRegistered = true
            return true
        }
        return false
    }
}