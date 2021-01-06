package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*
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
    val columnName = stripSeachName(statementAnalyzer.word() ?: throw SQLException("Unexpected "))
    val column: Column =
        tables.values.map { table -> table.findColumn(columnName) }.filterNotNull().firstOrNull() ?: throw SQLException(
            "Unknown column $columnName"
        )
    statementAnalyzer.addIndex()
    return when (statementAnalyzer.word()) {
        "=" -> EqualCase(column, nextIndexToUse, statementAnalyzer, dbTransaction, tables)
        ">" -> GreaterThanCause(column, nextIndexToUse, statementAnalyzer, dbTransaction, tables)
        "<" -> LessThanCause(column, nextIndexToUse, statementAnalyzer, dbTransaction, tables)
        "is" -> when {
            statementAnalyzer.word(1) == "distinct" && statementAnalyzer.word(2) == "from" -> {
                statementAnalyzer.addIndex(2)
                NotEqualCause(column, nextIndexToUse, statementAnalyzer, dbTransaction, tables)
            }
            statementAnalyzer.addIndex().word() == "null" -> IsNullClause(column)
            statementAnalyzer.word() == "not" && statementAnalyzer.addIndex()
                .word() == "null" -> IsNotNullClause(column)
            else -> throw SQLException("Syntax error after is")
        }
        "in" -> InClause(column, statementAnalyzer)
        else -> throw SQLException("Syntax error in where clause. Not recognicing word ${statementAnalyzer.word()}")
    }
}


interface WhereClause {
    fun isMatch(cells:List<Cell>):Boolean
    fun registerBinding(index:Int,value:Any?):Boolean
}

class MatchAllClause:WhereClause {
    override fun isMatch(cells: List<Cell>): Boolean {
        return true
    }

    override fun registerBinding(index: Int, value: Any?): Boolean {
        return false
    }


}