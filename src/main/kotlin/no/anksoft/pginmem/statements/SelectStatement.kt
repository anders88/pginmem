package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import no.anksoft.pginmem.clauses.WhereClause
import no.anksoft.pginmem.clauses.createWhereClause
import no.anksoft.pginmem.statements.select.*
import java.sql.ResultSet
import java.sql.SQLException

class OrderPart(val column: Column,val ascending:Boolean,val nullsFirst:Boolean)

private fun computeOrderParts(statementAnalyzer: StatementAnalyzer,tablesUsed:Map<String,Table>):List<OrderPart> {
    if (statementAnalyzer.word() != "order") {
        return emptyList()
    }
    if (statementAnalyzer.addIndex().word() != "by") {
        throw SQLException("Expected by after order")
    }
    val orderPats:MutableList<OrderPart> = mutableListOf()
    while (true) {
        statementAnalyzer.addIndex()
        val colnameText = statementAnalyzer.word()?:break
        val column:Column = statementAnalyzer.findColumnFromIdentifier(colnameText,tablesUsed)
        var nextWord = statementAnalyzer.addIndex().word()

        var ascending:Boolean = true
        if (nextWord == "asc") {
            statementAnalyzer.addIndex()
        } else if (nextWord == "desc") {
            ascending = false
            statementAnalyzer.addIndex()
        }
        val nullsFirst:Boolean = if (statementAnalyzer.word() == "nulls") {
            statementAnalyzer.addIndex()
            if (!setOf("first","last").contains(statementAnalyzer.addIndex().word())) {
                throw SQLException("Expected first or last")
            }
            val res = (statementAnalyzer.word() == "first")
            statementAnalyzer.addIndex()
            res
        } else true
        orderPats.add(OrderPart(column,ascending,nullsFirst))
    }
    return orderPats
}

private class SelectAnalyze(val selectedColumns:List<SelectColumnProvider>,val selectRowProvider: SelectRowProvider,val whereClause: WhereClause,val orderParts:List<OrderPart>)

private fun analyseSelect(statementAnalyzer:StatementAnalyzer, dbTransaction: DbTransaction):SelectAnalyze {
    val fromInd = statementAnalyzer.indexOf("from")
    val tablesUsed:Map<String,Table> = if (fromInd != -1) {
        val mappingTablesUsed:MutableMap<String,Table> = mutableMapOf()
        var tabind = fromInd+1
        while (!setOf("where","order").contains(statementAnalyzer.wordAt(tabind)?:"where")) {
            val table = dbTransaction.tableForRead(stripSeachName(statementAnalyzer.wordAt(tabind)?:""))
            tabind++
            val nextWord = statementAnalyzer.wordAt(tabind)
            val alias = if (nextWord != null && nextWord != "where" && nextWord != "," && nextWord != "order") {
                tabind++
                nextWord
            } else table.name
            mappingTablesUsed.put(alias,table)

            if (statementAnalyzer.wordAt(tabind) == ",") {
                tabind++
            }
        }
        mappingTablesUsed
    } else emptyMap()


    val allColumns:List<Column> = tablesUsed.map { it.value.colums }.flatten()
    statementAnalyzer.addIndex()

    val selectedColumns:List<SelectColumnProvider> = if (statementAnalyzer.word() == "*") allColumns.mapIndexed { index, column -> SelectDbColumn(column,index+1) } else {
        val addedSelected:MutableList<SelectColumnProvider> = mutableListOf()
        var selectcolindex = 0
        while ((statementAnalyzer.word()?:"from") != "from") {
            val valueFromExpression:ValueFromExpression = statementAnalyzer.readValueFromExpression(dbTransaction,tablesUsed)?:throw SQLException("Unknown select column")
            selectcolindex++
            addedSelected.add(ReadExpressionSelectColumnProvider(valueFromExpression,selectcolindex,dbTransaction))
            if (statementAnalyzer.addIndex().word() == ",") {
                statementAnalyzer.addIndex()
            }
        }
        addedSelected
    }


    /*
    val selectedColumnsx:List<SelectColumnProvider> = if (statementAnalyzer.word() == "*") allColumns.mapIndexed { index, column -> SelectDbColumn(column,index+1) } else {
        var ind = 1
        var colindex = 1

        val addedSelected:MutableList<SelectColumnProvider> = mutableListOf()
        val loopUntil = if (fromInd != -1) fromInd else statementAnalyzer.size
        while (ind < loopUntil) {
            val readColName = statementAnalyzer.wordAt(ind)!!
            val colname = stripSeachName(readColName)
            if (colname == "nextval" && ind+3 < loopUntil && statementAnalyzer.wordAt(ind+1) == "(" && statementAnalyzer.wordAt(ind+3) == ")" && statementAnalyzer.wordAt(ind+2)!!.length >= 3 && statementAnalyzer.wordAt(ind+2)!!.startsWith("'") and (statementAnalyzer.wordAt(ind+2)?:"").endsWith("'")) {
                val sequence:Sequence = dbTransaction.sequence(statementAnalyzer.wordAt(ind+2)!!.substring(1,statementAnalyzer.wordAt(ind+2)!!.length-1))
                addedSelected.add(SelectFromSequence(sequence,colindex))
                colindex++
                ind+=4
                continue
            }
            val column:Column = allColumns.firstOrNull { it.matches(it.tablename,colname) }?:throw SQLException("Unknown column ${statementAnalyzer.wordAt(ind)}")
            addedSelected.add(SelectDbColumn(column,colindex))
            colindex++
            ind+=2
        }
        addedSelected
    }*/

    while (!setOf("where","order").contains(statementAnalyzer.word()?:"where")) {
        statementAnalyzer.addIndex()
    }
    val whereClause:WhereClause = createWhereClause(statementAnalyzer,tablesUsed,1,dbTransaction)
    val orderParts = computeOrderParts(statementAnalyzer,tablesUsed)

    val selectRowProvider:SelectRowProvider = if (fromInd != -1) TablesSelectRowProvider(tablesUsed.values.toList(),whereClause,orderParts) else ImplicitOneRowSelectProvider()


    return SelectAnalyze(selectedColumns,selectRowProvider,whereClause,orderParts)

    /*val whereClause:WhereClause = createWhereClause(statementAnalyzer.setIndex(fromInd+2), listOfNotNull(usedTable),1)
    val selectRowProvider = if (usedTable != null) TablesSelectRowProvider(usedTable,whereClause) else ImplicitOneRowSelectProvider()
    return SelectAnalyze(selectedColumns,selectRowProvider,whereClause)*/
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