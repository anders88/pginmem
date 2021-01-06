package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*
import java.sql.SQLException

abstract class BinaryClause :WhereClause {

    private val column: Column
    private val expectedIndex:Int?
    private var valueToMatch:Any? = null
    private var isRegistered:Boolean
    private val valueFromExpression: ValueFromExpression?
    private val dbTransaction: DbTransaction

    constructor(column: Column, expectedIndex:IndexToUse, statementAnalyzer: StatementAnalyzer, dbTransaction: DbTransaction, tables:Map<String, Table>) {
        this.column = column
        this.dbTransaction = dbTransaction
        statementAnalyzer.addIndex()
        if (statementAnalyzer.word() == "?") {
            this.isRegistered = false
            this.valueFromExpression = null
            this.expectedIndex = expectedIndex.takeInd()
        } else {
            this.valueFromExpression = statementAnalyzer.readValueFromExpression(dbTransaction,tables)
            this.isRegistered = true
            this.expectedIndex = null
        }
    }


    override fun isMatch(cells: List<Cell>): Boolean {
        if (!isRegistered) {
            throw SQLException("Binding not set")
        }
        if (valueFromExpression != null) {
            valueToMatch = valueFromExpression.valuegen.invoke(Pair(dbTransaction, Row(cells)))
        }
        val cell: Cell = cells.firstOrNull { it.column == column }?:return false
        return matchValues(cell.value,valueToMatch)
    }

    abstract fun matchValues(left:Any?,right:Any?):Boolean

    override fun registerBinding(index: Int, value: Any?):Boolean {
        if (expectedIndex == index) {
            val givenValue = column.columnType.validateValue(value)
            valueToMatch = givenValue
            isRegistered = true
            return true
        }
        return false
    }

}