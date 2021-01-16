package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*
import no.anksoft.pginmem.values.CellValue
import no.anksoft.pginmem.values.NullCellValue
import java.sql.SQLException

private class InItem(
    val index:Int?,
    val valueFromExpression: ValueFromExpression?) {
    var givenValue:CellValue? = null
}

class InClause(
    val dbTransaction: DbTransaction,
    val leftValueFromExpression: ValueFromExpression,
    statementAnalyzer: StatementAnalyzer,
    nextIndexToUse: IndexToUse,
    tables: Map<String, Table>
):WhereClause {
    private val inValues:List<InItem>

    init {
        if (statementAnalyzer.addIndex().word() != "(") {
            throw SQLException("Expected ( in in")
        }
        val givenValue:MutableList<InItem> = mutableListOf()
        while (true) {
            statementAnalyzer.addIndex()
            if (statementAnalyzer.word() == ")") {
                break
            }
            if (statementAnalyzer.word() == ",") {
                statementAnalyzer.addIndex()
            }
            val aword = statementAnalyzer.word()

            val inItem:InItem = if (aword == "?") {
                InItem(nextIndexToUse.takeInd(),null)
            } else {
                val valueFromExpression = statementAnalyzer.readValueFromExpression(dbTransaction,tables)
                InItem(null,valueFromExpression)
            }
            givenValue.add(inItem)
        }
        inValues = givenValue
    }

    override fun isMatch(cells: List<Cell>): Boolean {
        if (inValues.any { it.index != null && it.givenValue == null }) {
            throw SQLException("Binding not registered")
        }
        val value:CellValue = leftValueFromExpression.valuegen.invoke(Pair(dbTransaction,Row(cells)))
        for (initem in inValues) {
            val invalue:CellValue = initem.givenValue?:
                initem.valueFromExpression?.valuegen?.invoke(Pair(dbTransaction,Row(cells)))?:
                NullCellValue
            if (invalue == value) {
                return true
            }
        }
        return false
    }

    override fun registerBinding(index: Int, value: CellValue): Boolean {
        val inItem:InItem = inValues.firstOrNull { it.index == index }?:return false
        inItem.givenValue = value
        return true
    }
}