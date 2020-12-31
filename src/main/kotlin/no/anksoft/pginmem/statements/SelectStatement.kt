package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import no.anksoft.pginmem.clauses.WhereClause
import no.anksoft.pginmem.clauses.createWhereClause
import no.anksoft.pginmem.statements.select.*
import java.sql.ResultSet
import java.sql.SQLException

private class SelectAnalyze(val selectedColumns:List<SelectColumnProvider>,val selectRowProvider: SelectRowProvider,val whereClause: WhereClause)

private fun analyseSelect(statementAnalyzer:StatementAnalyzer, dbTransaction: DbTransaction):SelectAnalyze {
    val fromInd = statementAnalyzer.indexOf("from")
    val usedTable:Table?  = if (fromInd != -1) dbTransaction.tableForRead(statementAnalyzer.wordAt(fromInd+1)!!) else null



    val allColumns = usedTable?.colums?: emptyList()

    val selectedColumns:List<SelectColumnProvider> = if (statementAnalyzer.wordAt(1) == "*") allColumns.mapIndexed { index, column -> SelectDbColumn(column,index+1) } else {
        var ind = 1
        var colindex = 1

        val addedSelected:MutableList<SelectColumnProvider> = mutableListOf()
        val loopUntil = if (fromInd != -1) fromInd else statementAnalyzer.size
        while (ind < loopUntil) {
            val colname = stripSeachName(statementAnalyzer.wordAt(ind)!!)
            if (colname == "nextval" && ind+3 < loopUntil && statementAnalyzer.wordAt(ind+1) == "(" && statementAnalyzer.wordAt(ind+3) == ")" && statementAnalyzer.wordAt(ind+2)!!.length >= 3 && statementAnalyzer.wordAt(ind+2)!!.startsWith("'") and (statementAnalyzer.wordAt(ind+2)?:"").endsWith("'")) {
                val sequence:Sequence = dbTransaction.sequence(statementAnalyzer.wordAt(ind+2)!!.substring(1,statementAnalyzer.wordAt(ind+2)!!.length-1))
                addedSelected.add(SelectFromSequence(sequence,colindex))
                colindex++
                ind+=4
                continue
            }
            val column:Column = allColumns.firstOrNull { it.name == colname }?:throw SQLException("Unknown column ${statementAnalyzer.wordAt(ind)}")
            addedSelected.add(SelectDbColumn(column,colindex))
            colindex++
            ind+=2
        }
        addedSelected
    }

    val whereClause:WhereClause = createWhereClause(statementAnalyzer.setIndex(fromInd+2), listOfNotNull(usedTable),1)
    val selectRowProvider = if (usedTable != null) TablesSelectRowProvider(usedTable,whereClause) else ImplicitOneRowSelectProvider()
    return SelectAnalyze(selectedColumns,selectRowProvider,whereClause)
}

class SelectStatement(statementAnalyzer: StatementAnalyzer, dbTransaction: DbTransaction):DbPreparedStatement() {
    private val selectAnalyze:SelectAnalyze = analyseSelect(statementAnalyzer,dbTransaction)




    override fun executeQuery(): ResultSet = internalExecuteQuery()

    fun internalExecuteQuery():SelectResultSet = SelectResultSet(selectAnalyze.selectedColumns,selectAnalyze.selectRowProvider)

    override fun setString(parameterIndex: Int, x: String?) {
        selectAnalyze.whereClause.registerBinding(parameterIndex,x)
    }

    override fun setInt(parameterIndex: Int, x: Int) {
        selectAnalyze.whereClause.registerBinding(parameterIndex,x)
    }

    fun setSomething(parameterIndex: Int, x: Any?) {
        selectAnalyze.whereClause.registerBinding(parameterIndex,x)
    }

}