package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*
import no.anksoft.pginmem.statements.SelectStatement
import no.anksoft.pginmem.statements.select.TableInSelect
import no.anksoft.pginmem.values.CellValue
import no.anksoft.pginmem.values.NullCellValue
import java.sql.SQLException

private class InItem(
    val index:Int?,
    val valueFromExpression: ValueFromExpression?) {
    var givenValue:CellValue? = null
}

class InClause constructor(
    val dbTransaction: DbTransaction,
    val leftValueFromExpression: ValueFromExpression,
    statementAnalyzer: StatementAnalyzer,
    nextIndexToUse: IndexToUse,
    tables: Map<String, TableInSelect>,
    val isNotIn:Boolean
):WhereClause {
    private val inValues:List<InItem>
    private val selectStatement:SelectStatement?

    init {
        if (statementAnalyzer.addIndex(if (isNotIn) 2 else 1).word() != "(") {
            throw SQLException("Expected ( in in")
        }
        val givenValue:MutableList<InItem> = mutableListOf()
        if (statementAnalyzer.word(1) == "select") {
            val selectsa = statementAnalyzer.extractParantesStepForward()?:throw SQLException("Expected end of select")
            selectStatement = SelectStatement(selectsa,dbTransaction,nextIndexToUse)
        } else {
            selectStatement = null
            while (true) {
                statementAnalyzer.addIndex()
                if (statementAnalyzer.word() == ")") {
                    break
                }
                if (statementAnalyzer.word() == ",") {
                    statementAnalyzer.addIndex()
                }
                val aword = statementAnalyzer.word()

                val inItem: InItem = if (aword == "?") {
                    InItem(nextIndexToUse.takeInd(), null)
                } else {
                    val valueFromExpression = statementAnalyzer.readValueFromExpression(dbTransaction, tables,nextIndexToUse)
                    InItem(null, valueFromExpression)
                }
                givenValue.add(inItem)
            }
        }
        inValues = givenValue
    }

    override fun isMatch(cells: List<Cell>): Boolean {
        if (inValues.any { it.index != null && it.givenValue == null }) {
            throw SQLException("Binding not registered")
        }
        val value:CellValue = leftValueFromExpression.genereateValue(dbTransaction,Row(cells))
        if (selectStatement != null) {
            val queryRes = selectStatement.internalExecuteQuery()
            for (rowindex in 0 until queryRes.numberOfRows) {
                val valueAt = queryRes.valueAt(1, rowindex)
                if (valueAt == value) {
                    return !isNotIn
                }
            }
        }
        for (initem in inValues) {
            val invalue:CellValue = initem.givenValue?:
                initem.valueFromExpression?.genereateValue(dbTransaction,Row(cells))?: NullCellValue
            if (invalue == value) {
                return !isNotIn
            }
        }
        return isNotIn
    }

    override fun registerBinding(index: Int, value: CellValue): Boolean {
        if (selectStatement?.registerBinding(index, value) == true) {
            return true
        }
        val inItem:InItem = inValues.firstOrNull { it.index == index }?:return false
        inItem.givenValue = value
        return true
    }
}