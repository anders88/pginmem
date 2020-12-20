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
    val usedTable:Table?  = if (fromInd != -1) dbTransaction.tableForRead(words[fromInd+1]) else null



    val allColumns = usedTable?.colums?: emptyList()

    val selectedColumns:List<SelectColumnProvider> = if (words[1] == "*") allColumns.mapIndexed { index, column -> SelectDbColumn(column,index+1) } else {
        var ind = 1
        var colindex = 1

        val addedSelected:MutableList<SelectColumnProvider> = mutableListOf()
        val loopUntil = if (fromInd != -1) fromInd else words.size
        while (ind < loopUntil) {
            val colname = stripSeachName(words[ind])
            if (colname == "nextval" && ind+3 < loopUntil && words[ind+1] == "(" && words[ind+3] == ")" && words[ind+2].length >= 3 && words[ind+2].startsWith("'") and words[ind+2].endsWith("'")) {
                val sequence:Sequence = dbTransaction.sequence(words[ind+2].substring(1,words[ind+2].length-1))
                addedSelected.add(SelectFromSequence(sequence,colindex))
                colindex++
                ind+=4
                continue
            }
            val column:Column = allColumns.firstOrNull { it.name == colname }?:throw SQLException("Unknown column ${words[ind]}")
            addedSelected.add(SelectDbColumn(column,colindex))
            colindex++
            ind+=2
        }
        addedSelected
    }

    val whereClause:WhereClause = if (usedTable != null && fromInd+3 < words.size) createWhereClause(words.subList(fromInd+3,words.size), listOf(usedTable),1) else MatchAllClause()
    val selectRowProvider = if (usedTable != null) TablesSelectRowProvider(usedTable,whereClause) else ImplicitOneRowSelectProvider()
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