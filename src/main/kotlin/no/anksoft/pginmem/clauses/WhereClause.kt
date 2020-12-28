package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.*
import java.sql.SQLException

fun createWhereClause(words:List<String>,tables:List<Table>,nextIndexToUse:Int):WhereClause {
    val columns:List<Column> = tables.map { it.colums }.flatten()
    val columnName = stripSeachName(getFromWords(words,0))
    val column:Column = columns.firstOrNull { it.name == columnName}?:throw SQLException("Unknown column $columnName")
    return when(getFromWords(words,1)) {
        "=" -> EqualCase(column,nextIndexToUse)
        ">" -> GreaterThanCause(column,nextIndexToUse)
        "<" -> LessThanCause(column,nextIndexToUse)
        else -> throw SQLException("Syntax error in where clause")
    }

}

fun createWhereClause(statementAnalyzer: StatementAnalyzer,tables:List<Table>,nextIndexToUse:Int):WhereClause {
    if (statementAnalyzer.word() != "where") {
        return MatchAllClause()
    }
    statementAnalyzer.addIndex()

    val columns:List<Column> = tables.map { it.colums }.flatten()
    val columnName = stripSeachName(statementAnalyzer.word()?:throw SQLException("Unexpected "))
    val column:Column = columns.firstOrNull { it.name == columnName}?:throw SQLException("Unknown column $columnName")
    statementAnalyzer.addIndex()
    return when(statementAnalyzer.word()) {
        "=" -> EqualCase(column,nextIndexToUse)
        ">" -> GreaterThanCause(column,nextIndexToUse)
        "<"  -> LessThanCause(column,nextIndexToUse)
        "is" -> when {
            statementAnalyzer.addIndex().word() == "null" -> IsNullClause(column)
            statementAnalyzer.word() == "not" && statementAnalyzer.addIndex().word() == "null" -> IsNotNullClause(column)
            else -> throw SQLException("Syntax error after is")
        }
        "in" -> InClause(column,statementAnalyzer)
        else -> throw SQLException("Syntax error in where clause. Not recognicing word ${statementAnalyzer.word()}")
    }

}



private fun <T> getFromWords(words: List<T>,index:Int):T {
    if (index < 0 || index >= words.size) {
        throw SQLException("Unexpected end of statement")
    }
    return words[index]
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