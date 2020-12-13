package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import no.anksoft.pginmem.clauses.MatchAllClause
import no.anksoft.pginmem.clauses.WhereClause
import no.anksoft.pginmem.clauses.createWhereClause
import java.sql.ResultSet
import java.sql.SQLException

private class SelectAnalyze(val selectedColumns:List<Pair<Column,Table>>,val allColumns:List<Pair<Column,Table>>,val whereClause: WhereClause, val usedTable:Table)

private fun analyseSelect(words: List<String>, dbTransaction: DbTransaction,sql:String):SelectAnalyze {
    val fromInd = words.indexOf("from")
    if (fromInd == -1) {
        throw SQLException("Expected from keyword in select $sql")
    }
    val usedTable:Table  = dbTransaction.tableForRead(words[fromInd+1]?:throw SQLException("Unknown table ${words[fromInd+1]}"))

    val allColumns:List<Pair<Column,Table>> = usedTable.colums.map { Pair(it,usedTable) }

    val selectedColumns:List<Pair<Column,Table>> = if (words[1] == "*") allColumns else {
        var ind = 1
        val addedSelected:MutableList<Pair<Column,Table>> = mutableListOf()
        while (ind < fromInd) {
            val colname = stripSeachName(words[ind])
            addedSelected.add(allColumns.firstOrNull { it.first.name == colname }?:throw SQLException("Unknown column ${words[ind]}"))
            ind+=2
        }
        addedSelected
    }

    val whereClause:WhereClause = if (fromInd+3 < words.size) createWhereClause(words.subList(fromInd+3,words.size), listOf(usedTable),1) else MatchAllClause()
    return SelectAnalyze(selectedColumns,allColumns,whereClause,usedTable)
}

class SelectStatement(words: List<String>, dbTransaction: DbTransaction,private val sqlOrig:String):DbPreparedStatement() {
    private val selectAnalyze:SelectAnalyze = analyseSelect(words,dbTransaction,sqlOrig)




    override fun executeQuery(): ResultSet {
        val rows:List<List<Cell>> = selectAnalyze.usedTable.rowsForReading().filter { selectAnalyze.whereClause.isMatch(it.cells) }.map { row ->
            selectAnalyze.selectedColumns.map { selColumn ->
                row.cells.first { it.column.name == selColumn.first.name }
            }
        }

        return SelectResultSet(selectAnalyze.selectedColumns.map { it.first },rows)
    }

    override fun setString(parameterIndex: Int, x: String?) {
        selectAnalyze.whereClause.registerBinding(parameterIndex,x)
    }

    override fun setInt(parameterIndex: Int, x: Int) {
        selectAnalyze.whereClause.registerBinding(parameterIndex,x)
    }

}