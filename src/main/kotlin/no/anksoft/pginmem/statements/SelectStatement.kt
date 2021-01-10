package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import no.anksoft.pginmem.clauses.WhereClause
import no.anksoft.pginmem.clauses.createWhereClause
import no.anksoft.pginmem.statements.select.*
import no.anksoft.pginmem.values.CellValue
import no.anksoft.pginmem.values.IntegerCellValue
import no.anksoft.pginmem.values.NullCellValue
import no.anksoft.pginmem.values.StringCellValue
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

private fun analyseSelect(statementAnalyzer:StatementAnalyzer, dbTransaction: DbTransaction,startOnWhereClauseBindingNo:Int):SelectAnalyze {
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

    val aliasMapping:Map<String,String> = tablesUsed.mapValues { it.value.name }

    val allColumns:List<Column> = tablesUsed.map { it.value.colums }.flatten()
    statementAnalyzer.addIndex()

    val selectedColumns:List<SelectColumnProvider> = if (statementAnalyzer.word() == "*") allColumns.mapIndexed { index, column -> SelectDbColumn(column,index+1,aliasMapping) } else {
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


    while (!setOf("where","order").contains(statementAnalyzer.word()?:"where")) {
        statementAnalyzer.addIndex()
    }
    val whereClause:WhereClause = createWhereClause(statementAnalyzer,tablesUsed,startOnWhereClauseBindingNo,dbTransaction)
    val orderParts = computeOrderParts(statementAnalyzer,tablesUsed)

    val selectRowProvider:SelectRowProvider = if (fromInd != -1) TablesSelectRowProvider(tablesUsed.values.toList(),whereClause,orderParts) else ImplicitOneRowSelectProvider()


    return SelectAnalyze(selectedColumns,selectRowProvider,whereClause,orderParts)

}

class SelectStatement(statementAnalyzer: StatementAnalyzer, dbTransaction: DbTransaction,startOnWhereClauseBindingNo: Int = 1):DbPreparedStatement() {
    private val selectAnalyze:SelectAnalyze = analyseSelect(statementAnalyzer,dbTransaction,startOnWhereClauseBindingNo)




    override fun executeQuery(): ResultSet = internalExecuteQuery()

    fun internalExecuteQuery():SelectResultSet = SelectResultSet(selectAnalyze.selectedColumns,selectAnalyze.selectRowProvider)

    override fun setString(parameterIndex: Int, x: String?) {
        selectAnalyze.whereClause.registerBinding(parameterIndex,if (x == null) NullCellValue else StringCellValue(x))
    }

    override fun setInt(parameterIndex: Int, x: Int) {
        selectAnalyze.whereClause.registerBinding(parameterIndex,IntegerCellValue(x.toLong()))
    }

    fun setSomething(parameterIndex: Int, x: CellValue) {
        selectAnalyze.whereClause.registerBinding(parameterIndex,x)
    }

}