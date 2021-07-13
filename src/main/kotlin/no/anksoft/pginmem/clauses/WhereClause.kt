package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*
import no.anksoft.pginmem.values.CellValue
import java.sql.SQLException



fun createWhereClause(statementAnalyzer: StatementAnalyzer,tables:Map<String,Table>,indexToUse: IndexToUse,dbTransaction: DbTransaction):WhereClause {
    if (statementAnalyzer.word() != "where") {
        return MatchAllClause()
    }
    statementAnalyzer.addIndex()
    val nextIndexToUse = indexToUse

    return parseWhereClause(nextIndexToUse, statementAnalyzer, tables, dbTransaction)


}

private fun parseWhereClause(
    nextIndexToUse: IndexToUse,
    statementAnalyzer: StatementAnalyzer,
    tables: Map<String, Table>,
    dbTransaction: DbTransaction
): WhereClause {

    var leftAndPart: WhereClause? = null
    var isAndNotOr: Boolean = true
    while (true) {
        val indAtStart = nextIndexToUse.nextIndex
        val innerStatementAnalyzer:StatementAnalyzer? = if (statementAnalyzer.word() == "(") statementAnalyzer.extractParantesStepForward() else null
        val nextPart = if (innerStatementAnalyzer != null) {
            parseWhereClause(nextIndexToUse,innerStatementAnalyzer,tables,dbTransaction)
        } else readWhereClausePart(statementAnalyzer, tables, nextIndexToUse, dbTransaction)

        leftAndPart = if (leftAndPart != null) {
            if (isAndNotOr) AndClause(leftAndPart, nextPart, indAtStart) else OrClause(
                leftAndPart,
                nextPart,
                indAtStart
            )
        } else nextPart

        if (setOf("and", "or").contains(statementAnalyzer.addIndex().word())) {
            isAndNotOr = statementAnalyzer.word() == "and"
        } else {
            return leftAndPart
        }
        statementAnalyzer.addIndex()

    }

}


class IndexToUse(private var index:Int=1) {
    fun takeInd():Int {
        val res = index
        index++
        return res
    }

    val nextIndex:Int get() = index

}

private fun readWhereClausePart(
    statementAnalyzer: StatementAnalyzer,
    tables: Map<String, Table>,
    nextIndexToUse: IndexToUse,
    dbTransaction: DbTransaction
): WhereClause {

    val leftValueFromExpression = statementAnalyzer.readValueFromExpression(dbTransaction,tables)

    statementAnalyzer.addIndex()
    return when (statementAnalyzer.word()) {
        "=" -> EqualCase(leftValueFromExpression, nextIndexToUse, statementAnalyzer, dbTransaction, tables)
        ">" -> GreaterThanCause(leftValueFromExpression, nextIndexToUse, statementAnalyzer, dbTransaction, tables)
        "<" -> LessThanCause(leftValueFromExpression, nextIndexToUse, statementAnalyzer, dbTransaction, tables)
        "<>" -> NotEqualCause(leftValueFromExpression, nextIndexToUse, statementAnalyzer, dbTransaction, tables)
        "is" -> when {
            statementAnalyzer.word(1) == "distinct" && statementAnalyzer.word(2) == "from" -> {
                statementAnalyzer.addIndex(2)
                NotEqualCause(leftValueFromExpression, nextIndexToUse, statementAnalyzer, dbTransaction, tables)
            }
            statementAnalyzer.word(1) == "null" -> {
                statementAnalyzer.addIndex()
                IsNullClause(dbTransaction,leftValueFromExpression)
            }
            statementAnalyzer.word(1) == "not" && statementAnalyzer.word(2) == "null" -> {
                statementAnalyzer.addIndex(2)
                IsNotNullClause(dbTransaction,leftValueFromExpression)
            }
            statementAnalyzer.word(1) == "true" -> EqualCase(leftValueFromExpression, nextIndexToUse, statementAnalyzer, dbTransaction, tables)
            statementAnalyzer.word(1) == "false" -> EqualCase(leftValueFromExpression, nextIndexToUse, statementAnalyzer, dbTransaction, tables)
            else -> throw SQLException("Syntax error after is")
        }
        "in" -> InClause(dbTransaction,leftValueFromExpression, statementAnalyzer,nextIndexToUse,tables)
        else -> throw SQLException("Syntax error in where clause. Not recognicing word ${statementAnalyzer.word()}")
    }
}


interface WhereClause {
    fun isMatch(cells:List<Cell>):Boolean
    fun registerBinding(index:Int,value: CellValue):Boolean
}

class MatchAllClause:WhereClause {
    override fun isMatch(cells: List<Cell>): Boolean {
        return true
    }

    override fun registerBinding(index: Int, value: CellValue): Boolean {
        return false
    }


}