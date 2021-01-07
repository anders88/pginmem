package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.DbTransaction
import no.anksoft.pginmem.ValueFromExpression
import no.anksoft.pginmem.stripSeachName

class ReadExpressionSelectColumnProvider(private val valueFromExpression: ValueFromExpression,colindex:Int, val dbTransaction: DbTransaction):SelectColumnProvider(colindex) {
    override fun match(identificator: String): Boolean {
        val column = valueFromExpression.column ?: return false
        return column.matches(column.tablename, stripSeachName(identificator))
    }

    override fun readValue(selectRowProvider: SelectRowProvider, rowindex: Int): Any? {
        val column = valueFromExpression.column
        if (column == null) {
            return valueFromExpression.valuegen.invoke(Pair(dbTransaction,null))
        }
        return selectRowProvider.readValue(column,rowindex)
    }
}