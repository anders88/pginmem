package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*
import no.anksoft.pginmem.values.CellValue
import java.sql.SQLException


fun createWhereClause(statementAnalyzer: StatementAnalyzer,tables:Map<String,Table>,startOnIndex:Int,dbTransaction: DbTransaction):WhereClause {
    if (statementAnalyzer.word() != "where") {
        return MatchAllClause()
    }
    statementAnalyzer.addIndex()


    while (true) {

        val nextIndexToUse = IndexToUse(startOnIndex)
        var leftAndPart:WhereClause? = null
        while (true) {
            val indAtStart = nextIndexToUse.nextIndex
            val nextPart = readWhereClausePart(statementAnalyzer, tables, nextIndexToUse, dbTransaction)

            leftAndPart = if (leftAndPart != null) {
                AndClause(leftAndPart,nextPart,indAtStart)
            } else nextPart

            if (statementAnalyzer.addIndex().word() != "and") {
                return leftAndPart
            }
            statementAnalyzer.addIndex()

        }

    }


}


class IndexToUse(private var index:Int) {
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
        "is" -> when {
            statementAnalyzer.word(1) == "distinct" && statementAnalyzer.word(2) == "from" -> {
                statementAnalyzer.addIndex(2)
                NotEqualCause(leftValueFromExpression, nextIndexToUse, statementAnalyzer, dbTransaction, tables)
            }
            statementAnalyzer.addIndex().word() == "null" -> IsNullClause(dbTransaction,leftValueFromExpression)
            statementAnalyzer.word() == "not" && statementAnalyzer.addIndex()
                .word() == "null" -> IsNotNullClause(dbTransaction,leftValueFromExpression)
            else -> throw SQLException("Syntax error after is")
        }
        "in" -> InClause(dbTransaction,leftValueFromExpression, statementAnalyzer)
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