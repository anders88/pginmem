package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.*
import no.anksoft.pginmem.values.CellValue

class SelectColumnProvider(
    val colindex:Int,
    val alias:String?,
    val valueFromExpression: ValueFromExpression,
    val dbTransaction: DbTransaction,
    val aggregateFunction: AggregateFunction?,
    val tableAliases:Map<String,String>
    ) {
    fun readValue(selectRowProvider: SelectRowProvider,rowindex:Int):CellValue {
        val row = selectRowProvider.readRow(rowindex)
        return valueFromExpression.valuegen.invoke(Pair(dbTransaction,row))
    }

    fun isMatch(colidentifier:String):Boolean {
        if (alias != null) return (alias == colidentifier)

        val column:Column = valueFromExpression.column?:return false

        val dotind = colidentifier.indexOf(".")
        if (dotind == -1) {
            return column.matches(column.tablename, stripSeachName(colidentifier))
        }
        val givenTableName = stripSeachName(colidentifier.substring(0, dotind))
        val tablename:String = tableAliases[givenTableName]?:givenTableName
        val givenColname = stripSeachName(colidentifier.substring(dotind+1))
        return column.matches(tablename,givenColname)

    }
}
