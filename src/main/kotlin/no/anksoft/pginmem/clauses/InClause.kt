package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*
import no.anksoft.pginmem.values.CellValue
import no.anksoft.pginmem.values.StringCellValue
import java.sql.SQLException

class InClause(val dbTransaction: DbTransaction, val leftValueFromExpression: ValueFromExpression, statementAnalyzer: StatementAnalyzer):WhereClause {
    private val inValues:List<CellValue>

    init {
        if (statementAnalyzer.addIndex().word() != "(") {
            throw SQLException("Expected ( in in")
        }
        val givenValue:MutableList<CellValue> = mutableListOf()
        while (true) {
            statementAnalyzer.addIndex()
            if (statementAnalyzer.word() == ")") {
                break
            }
            if (statementAnalyzer.word() == ",") {
                statementAnalyzer.addIndex()
            }
            val aword = statementAnalyzer.word()
            if (!(aword?.startsWith("'") == true && aword.endsWith("'"))) {
                throw SQLException("Only text supported in in clause")
            }
            givenValue.add(StringCellValue(aword.substring(1,aword.length-1)))
        }
        inValues = givenValue
    }

    override fun isMatch(cells: List<Cell>): Boolean {
        val value = leftValueFromExpression.valuegen.invoke(Pair(dbTransaction,Row(cells)))
        return inValues.contains(value)
    }

    override fun registerBinding(index: Int, value: CellValue): Boolean {
        return false
    }
}