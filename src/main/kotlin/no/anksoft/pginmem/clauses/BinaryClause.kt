package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*
import no.anksoft.pginmem.values.CellValue
import no.anksoft.pginmem.values.NullCellValue
import java.sql.SQLException

abstract class BinaryClause(
    private val leftValueFromExpression: ValueFromExpression,
    expectedIndex: IndexToUse,
    statementAnalyzer: StatementAnalyzer,
    private val dbTransaction: DbTransaction,
    tables: Map<String, Table>
) :WhereClause {

    private val expectedIndex:Int?
    private var valueToMatch:CellValue = NullCellValue
    private var isRegistered:Boolean
    private val rightValue: ValueFromExpression?

    init {
        statementAnalyzer.addIndex()
        if (statementAnalyzer.word() == "?") {
            this.isRegistered = false
            this.rightValue = null
            this.expectedIndex = expectedIndex.takeInd()
        } else {
            this.rightValue = statementAnalyzer.readValueFromExpression(dbTransaction,tables)
            this.isRegistered = true
            this.expectedIndex = null
        }
    }


    override fun isMatch(cells: List<Cell>): Boolean {
        if (!isRegistered) {
            throw SQLException("Binding not set")
        }
        if (rightValue != null) {
            valueToMatch = rightValue.valuegen.invoke(Pair(dbTransaction, Row(cells)))
        }
        val leftValue = leftValueFromExpression.valuegen.invoke(Pair(dbTransaction,Row(cells)))
        return matchValues(leftValue,valueToMatch)
    }

    abstract fun matchValues(left:CellValue,right:CellValue):Boolean

    override fun registerBinding(index: Int, value: CellValue):Boolean {
        if (expectedIndex == index) {
            valueToMatch = value
            isRegistered = true
            return true
        }
        return false
    }

}