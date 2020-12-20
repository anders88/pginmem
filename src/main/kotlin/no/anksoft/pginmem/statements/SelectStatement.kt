package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import no.anksoft.pginmem.clauses.MatchAllClause
import no.anksoft.pginmem.clauses.WhereClause
import no.anksoft.pginmem.clauses.createWhereClause
import no.anksoft.pginmem.statements.select.*
import java.sql.ResultSet
import java.sql.SQLException

private class SelectAnalyze(val selectedColumns:List<SelectColumnProvider>,val selectRowProvider: SelectRowProvider,val whereClause: WhereClause)

private fun analyseSelect(words: List<String>, dbTransaction: DbTransaction,sql:String):SelectAnalyze {
    val fromInd = words.indexOf("from")
    if (fromInd == -1) {
        throw SQLException("Expected from keyword in select $sql")
    }
    val usedTable:Table  = dbTransaction.tableForRead(words[fromInd+1])



    val allColumns = usedTable.colums

    val selectedColumns:List<SelectColumnProvider> = if (words[1] == "*") allColumns.mapIndexed { index, column -> SelectDbColumn(column,index+1) } else {
        var ind = 1
        var colindex = 1

        val addedSelected:MutableList<SelectColumnProvider> = mutableListOf()
        while (ind < fromInd) {
            val colname = stripSeachName(words[ind])
            val column:Column = allColumns.firstOrNull { it.name == colname }?:throw SQLException("Unknown column ${words[ind]}")
            addedSelected.add(SelectDbColumn(column,colindex))
            colindex++
            ind+=2
        }
        addedSelected
    }

    val whereClause:WhereClause = if (fromInd+3 < words.size) createWhereClause(words.subList(fromInd+3,words.size), listOf(usedTable),1) else MatchAllClause()
    val selectRowProvider = TablesSelectRowProvider(usedTable,whereClause)
    return SelectAnalyze(selectedColumns,selectRowProvider,whereClause)
}

class SelectStatement(words: List<String>, dbTransaction: DbTransaction,private val sqlOrig:String):DbPreparedStatement() {
    private val selectAnalyze:SelectAnalyze = analyseSelect(words,dbTransaction,sqlOrig)




    override fun executeQuery(): ResultSet {
        return SelectResultSet(selectAnalyze.selectedColumns,selectAnalyze.selectRowProvider)
    }

    override fun setString(parameterIndex: Int, x: String?) {
        selectAnalyze.whereClause.registerBinding(parameterIndex,x)
    }

    override fun setInt(parameterIndex: Int, x: Int) {
        selectAnalyze.whereClause.registerBinding(parameterIndex,x)
    }

}