package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*
import no.anksoft.pginmem.statements.SelectStatement
import no.anksoft.pginmem.statements.select.TableInSelect
import no.anksoft.pginmem.values.CellValue
import java.sql.SQLException



fun createWhereClause(statementAnalyzer: StatementAnalyzer,tables:Map<String,TableInSelect>,indexToUse: IndexToUse,dbTransaction: DbTransaction):WhereClause {
    if (!setOf("where","when").contains(statementAnalyzer.word())) {
        return MatchAllClause()
    }
    statementAnalyzer.addIndex()
    val nextIndexToUse = indexToUse

    return parseWhereClause(nextIndexToUse, statementAnalyzer, tables, dbTransaction)


}

private fun parseWhereClause(
    nextIndexToUse: IndexToUse,
    statementAnalyzer: StatementAnalyzer,
    tables: Map<String, TableInSelect>,
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
    tables: Map<String, TableInSelect>,
    nextIndexToUse: IndexToUse,
    dbTransaction: DbTransaction
): WhereClause {

    if (statementAnalyzer.word() == "exists" || (statementAnalyzer.word() == "not" && statementAnalyzer.word(1) == "exists")) {
        val doesExsists = statementAnalyzer.word() == "exists"
        statementAnalyzer.addIndex(if (doesExsists) 1 else 2)
        if (statementAnalyzer.word() != "(") {
            throw SQLException("Expected ( after exsists")
        }
        val innerSelectAn =
            statementAnalyzer.extractParantesStepForward() ?: throw SQLException("Unexpected end after exsists")
        val selectStatement = SelectStatement(innerSelectAn, dbTransaction, nextIndexToUse)
        return ExsistsClause(selectStatement,doesExsists)
    }

    val leftValueFromExpression = statementAnalyzer.readValueFromExpression(dbTransaction,tables,nextIndexToUse)

    statementAnalyzer.addIndex()
    val giveAndAdd:(Pair<WhereClause,Int>)->WhereClause = {
        statementAnalyzer.addIndex(it.second)
        it.first
    }
    return when (statementAnalyzer.word()) {
        "=" -> if (statementAnalyzer.matchesWord(listOf("=","any","(","?",")")))
                AnyClause(leftValueFromExpression,nextIndexToUse,dbTransaction)
                else EqualCase(leftValueFromExpression, nextIndexToUse, statementAnalyzer, dbTransaction, tables)
        ">" -> GreaterThanCause(leftValueFromExpression, nextIndexToUse, statementAnalyzer, dbTransaction, tables)
        ">=" -> GreaterThanOrEqualCause(leftValueFromExpression, nextIndexToUse, statementAnalyzer, dbTransaction, tables)
        "<" -> LessThanCause(leftValueFromExpression, nextIndexToUse, statementAnalyzer, dbTransaction, tables)
        "<=" -> LessThanOrEqualCause(leftValueFromExpression, nextIndexToUse, statementAnalyzer, dbTransaction, tables)
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
            statementAnalyzer.matchesWord(listOf("is","true")) -> giveAndAdd(Pair(BooleanIsClause(leftValueFromExpression,dbTransaction,true,false),1))
            statementAnalyzer.matchesWord(listOf("is","false")) -> giveAndAdd(Pair(BooleanIsClause(leftValueFromExpression,dbTransaction,false,false),1))
            statementAnalyzer.matchesWord(listOf("is","not","false")) -> giveAndAdd(Pair(BooleanIsClause(leftValueFromExpression,dbTransaction,false,true),2))
            statementAnalyzer.matchesWord(listOf("is","not","true")) -> giveAndAdd(Pair(BooleanIsClause(leftValueFromExpression,dbTransaction,true,true),2))
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