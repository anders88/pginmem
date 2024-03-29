package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.*

class SelectColumnProvider constructor(
    val colindex: Int,
    val alias: String?,
    val valueFromExpression: ValueFromExpression,
    val aggregateFunction: AggregateFunction?,
    val tableAliases: Map<String, String>
) {

    fun isMatch(colidentifier:String):Boolean {
        //if (alias != null) return (alias == colidentifier)
        if (alias == colidentifier) {
            return true
        }

        val column:ColumnInSelect = valueFromExpression.column?:return false

        val dotind = colidentifier.indexOf(".")
        if (dotind == -1) {
            return column.matches(column.tablename, stripSeachName(colidentifier))
        }
        val givenTableName = stripSeachName(colidentifier.substring(0, dotind))
        val tablename:String = tableAliases[givenTableName]?:givenTableName
        val givenColname = stripSeachName(colidentifier.substring(dotind+1))
        return column.matches(tablename,givenColname)

    }

    override fun toString(): String {
        return "SelectColumnProvider(colindex=$colindex, alias=$alias)"
    }


}
