package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.Cell
import no.anksoft.pginmem.Column
import no.anksoft.pginmem.Table
import java.sql.SQLException

fun createWhereClause(words:List<String>,tables:List<Table>,nextIndexToUse:Int):WhereClause {
    val columns:List<Column> = tables.map { it.colums }.flatten()
    if (getFromWords(words,1) == "=") {
        val columnName = getFromWords(words,0)
        val column:Column = columns.firstOrNull { it.name == columnName}?:throw SQLException("Unknown column $columnName")
        return EqualCase(column,nextIndexToUse)
    }
    throw SQLException("Syntax error in where clause")
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