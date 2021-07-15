package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import no.anksoft.pginmem.clauses.IndexToUse
import no.anksoft.pginmem.clauses.WhereClause
import no.anksoft.pginmem.clauses.createWhereClause
import no.anksoft.pginmem.statements.select.*
import no.anksoft.pginmem.values.*
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Timestamp

class OrderPart(val column: ColumnInSelect,val ascending:Boolean,val nullsFirst:Boolean)

private fun computeOrderParts(statementAnalyzer: StatementAnalyzer,tablesUsed:Map<String,TableInSelect>):List<OrderPart> {
    if (statementAnalyzer.word() != "order") {
        return emptyList()
    }
    if (statementAnalyzer.addIndex().word() != "by") {
        throw SQLException("Expected by after order")
    }
    val orderPats:MutableList<OrderPart> = mutableListOf()
    while (true) {
        statementAnalyzer.addIndex()
        if (statementAnalyzer.word() == ",") {
            statementAnalyzer.addIndex()
        }
        val colnameText = statementAnalyzer.word()?:break
        if (setOf("group").contains(colnameText)) {
            break
        }
        val column:ColumnInSelect = statementAnalyzer.findColumnFromIdentifier(colnameText,tablesUsed)
        val nextWord = statementAnalyzer.addIndex().word()

        var ascending:Boolean = true
        if (nextWord == "asc") {
            statementAnalyzer.addIndex()
        } else if (nextWord == "desc") {
            ascending = false
            statementAnalyzer.addIndex()
        }
        val nullsFirst:Boolean = if (statementAnalyzer.word() == "nulls") {
            statementAnalyzer.addIndex()
            if (!setOf("first","last").contains(statementAnalyzer.word())) {
                throw SQLException("Expected first or last")
            }
            val res = (statementAnalyzer.word() == "first")
            statementAnalyzer.addIndex()
            res
        } else true
        orderPats.add(OrderPart(column,ascending,nullsFirst))
        if (setOf("fetch","limit").contains(statementAnalyzer.word())) {
            break
        }
    }
    return orderPats
}

class SelectAnalyze constructor(
    val selectedColumns:List<SelectColumnProvider>,
    val selectRowProvider: SelectRowProvider,val
    whereClause: WhereClause,val orderParts:List<OrderPart>,val distinctFlag:Boolean) {

    fun registerBinding(index:Int,value: CellValue):Boolean {
        for (sc in selectedColumns) {
            if (sc.valueFromExpression.registerBinding(index,value)) return true
        }
        return whereClause.registerBinding(index,value)
    }
}

class ValueGenFromDbCell(override val column: Column):ValueFromExpression {
    override val valuegen: (Pair<DbTransaction, Row?>) -> CellValue = {
        it.second?.cells?.firstOrNull { it.column == column }?.value?:NullCellValue
    }

    override fun registerBinding(index:Int,value: CellValue):Boolean = false
}


class SelectColumnValue(val selectAnalyze:SelectAnalyze):ValueFromExpression {
    override val valuegen: (Pair<DbTransaction, Row?>) -> CellValue = {
        val rowProvider = selectAnalyze.selectRowProvider.providerWithFixed(it.second)
        val selectResultSet = SelectResultSet(
            selectAnalyze.selectedColumns,
            rowProvider,
            it.first,
            selectAnalyze.distinctFlag
        )
        if (selectResultSet.numberOfRows < 1) {
            NullCellValue
        } else {
            selectResultSet.valueAt(1,0)
        }
    }

    override val column: Column? = null

    override fun registerBinding(index:Int,value: CellValue):Boolean = selectAnalyze.registerBinding(index,value)

}



private fun analyseSelect(statementAnalyzer:StatementAnalyzer, dbTransaction: DbTransaction,indexToUse: IndexToUse,givenTablesUsed:Map<String,TableInSelect>):SelectAnalyze {
    val fromInd = statementAnalyzer.indexOf("from")
    val myTablesUsed:Map<String,TableInSelect> = if (fromInd != -1) {
        val mappingTablesUsed:MutableMap<String,TableInSelect> = mutableMapOf()
        var tabind = fromInd+1
        while (!setOf("where","order","group",")").contains(statementAnalyzer.wordAt(tabind)?:"where")) {
            if (statementAnalyzer.wordAt(tabind) == "(") {
                val extractParensFromOffset = statementAnalyzer.extractParensFromOffset(tabind)
                if (extractParensFromOffset != null) {

                }
            }
            val table = dbTransaction.tableForRead(stripSeachName(statementAnalyzer.wordAt(tabind)?:""))
            tabind++
            val nextWord = statementAnalyzer.wordAt(tabind)
            val alias = if (nextWord != null && nextWord != "where" && nextWord != "," && nextWord != "order" && nextWord != "group" && nextWord != ")") {
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

    val tablesUsed:Map<String,TableInSelect> = myTablesUsed + givenTablesUsed

    val aliasMapping:Map<String,String> = tablesUsed.mapValues { it.value.name }

    val allColumns:List<ColumnInSelect> = tablesUsed.map { it.value.colums }.flatten()
    statementAnalyzer.addIndex()

    val distinctFlag:Boolean = if (statementAnalyzer.word() == "distinct") {
        statementAnalyzer.addIndex()
        true
    } else false

    val selectedColumns:List<SelectColumnProvider> = if (statementAnalyzer.word() == "*") {
        allColumns.mapIndexed { index, column ->
            SelectColumnProvider(
                colindex = index+1,
                alias = null,
                valueFromExpression = column.myValueFromExpression,
                aggregateFunction = null,
                tableAliases = aliasMapping
            )
        }
    } else {
        val addedSelected:MutableList<SelectColumnProvider> = mutableListOf()
        var selectcolindex = 0
        while (!setOf("from","where").contains(statementAnalyzer.word()?:"from")) {
            val aggregateFunction:AggregateFunction? = when (statementAnalyzer.word()) {
                "max" -> MaxAggregateFunction()
                "min" -> MinAggregateFunction()
                "sum" -> SumAggregateFunction()
                "count" -> CountAggregateFunction()
                else -> null
            }
            if (aggregateFunction != null) {
                if (statementAnalyzer.addIndex().word() != "(") {
                    throw SQLException("Expected ( after aggregate")
                }
                statementAnalyzer.addIndex()
            }
            val selword = statementAnalyzer.word()
            if (selword != "*" && selword?.endsWith("*") == true) {
                val aliasind = selword.indexOf(".")
                if (aliasind != selword.length-2) {
                    throw SQLException("Expected . before *")
                }
                val table:Table = dbTransaction.tableForRead(aliasMapping[selword.substring(0,aliasind)]?:throw SQLException("unknown columns $selword"))
                for (columnToSelect in table.colums) {
                    val valuegen:((Pair<DbTransaction,Row?>)->CellValue) = { it.second?.cells?.firstOrNull { it.column == columnToSelect }?.value?:NullCellValue }
                    val valueFromExpression = BasicValueFromExpression(valuegen,columnToSelect)
                    selectcolindex++
                    addedSelected.add(SelectColumnProvider(
                        colindex = selectcolindex,
                        alias = null,
                        valueFromExpression = valueFromExpression,
                        aggregateFunction = null,
                        tableAliases = aliasMapping
                    ))
                }
                statementAnalyzer.addIndex()
            } else {
                val valueFromExpression: ValueFromExpression = if (aggregateFunction is CountAggregateFunction) {
                    if (statementAnalyzer.word() != "*") {
                        throw SQLException("Only support count(*)")
                    }
                    BasicValueFromExpression({ IntegerCellValue(1) }, null)
                } else if (statementAnalyzer.word() == "(") {
                    val parantes = statementAnalyzer.extractParantesStepForward()?:throw SQLException("Could not read subquery in select")
                    val innerSelect = analyseSelect(parantes,dbTransaction,indexToUse,tablesUsed)
                    SelectColumnValue(innerSelect)
                } else statementAnalyzer.readValueFromExpression(dbTransaction, tablesUsed,indexToUse)

                if (aggregateFunction != null) {
                    if (statementAnalyzer.addIndex().word() != ")") {
                        throw SQLException("Expected ) after aggregate")
                    }
                }

                selectcolindex++
                statementAnalyzer.addIndex()
                val possibleAlias: String? = if (statementAnalyzer.word() == "as") {
                    val a = statementAnalyzer.addIndex().word() ?: throw SQLException("Expeted alias after as")
                    statementAnalyzer.addIndex()
                    a
                } else null
                val columnProvider = SelectColumnProvider(
                    colindex = selectcolindex,
                    alias = possibleAlias,
                    valueFromExpression = valueFromExpression,
                    aggregateFunction = aggregateFunction,
                    tableAliases = aliasMapping
                )
                addedSelected.add(columnProvider)
            }
            if (statementAnalyzer.word() == ",") {
                statementAnalyzer.addIndex()
            }
        }
        addedSelected
    }


    while (!setOf("where","order","group").contains(statementAnalyzer.word()?:"where")) {
        statementAnalyzer.addIndex()
    }
    val whereClause:WhereClause = createWhereClause(statementAnalyzer,tablesUsed,indexToUse,dbTransaction)

    while (statementAnalyzer.word()?:"order" != "order") {
        statementAnalyzer.addIndex()
    }
    val orderParts:List<OrderPart> = computeOrderParts(statementAnalyzer, tablesUsed)

    val (limitRowsTo:Int?,offsetRows:Int) = if (setOf("fetch","limit").contains(statementAnalyzer.word())) {
        val isFetch = (statementAnalyzer.word() == "fetch")
        if (isFetch) {
            if (!setOf("first","next").contains(statementAnalyzer.addIndex().word())) {
                throw SQLException("expecting first or next")
            }
        }
        statementAnalyzer.addIndex()
        val limit:Int = statementAnalyzer.word()?.toIntOrNull()?:throw SQLException("Limit must be numeric")

        val offset:Int = if (isFetch) 0 else {
            if (statementAnalyzer.word(1) == "offset") {
                statementAnalyzer.addIndex(2).word()?.toIntOrNull()?:throw SQLException("Expected numeric offset")
            } else 0
        }
        Pair(limit,offset)
    } else Pair(null,0)

    val selectRowProvider:SelectRowProvider = if (fromInd != -1) TablesSelectRowProvider(myTablesUsed.values.toList(),whereClause,orderParts,limitRowsTo,offsetRows) else ImplicitOneRowSelectProvider(whereClause)


    return SelectAnalyze(selectedColumns,selectRowProvider,whereClause,orderParts,distinctFlag)

}

class SelectStatement(statementAnalyzer: StatementAnalyzer, val dbTransaction: DbTransaction,indexToUse: IndexToUse= IndexToUse()):StatementWithSet(dbTransaction) {
    val selectAnalyze:SelectAnalyze = analyseSelect(statementAnalyzer,dbTransaction,indexToUse, emptyMap())

    fun registerBinding(index:Int,value: CellValue):Boolean = selectAnalyze.registerBinding(index,value)

    override fun setSomething(parameterIndex: Int, x: CellValue) {
        registerBinding(parameterIndex,x)
    }

    override fun executeQuery(): ResultSet = internalExecuteQuery()

    fun internalExecuteQuery():SelectResultSet = SelectResultSet(selectAnalyze.selectedColumns,selectAnalyze.selectRowProvider,dbTransaction,selectAnalyze.distinctFlag)



}