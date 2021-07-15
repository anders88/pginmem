package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.*

class SelectColumnProvider(
    val colindex: Int,
    private val alias: String?,
    val valueFromExpression: ValueFromExpression,
    val aggregateFunction: AggregateFunction?,
    val tableAliases: Map<String, String>
) {

    fun isMatch(colidentifier:String):Boolean {
        if (alias != null) return (alias == colidentifier)

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
}
