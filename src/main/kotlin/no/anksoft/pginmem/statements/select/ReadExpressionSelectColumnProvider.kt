package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.DbTransaction
import no.anksoft.pginmem.ValueFromExpression
import no.anksoft.pginmem.stripSeachName

class ReadExpressionSelectColumnProvider(private val valueFromExpression: ValueFromExpression,colindex:Int, val dbTransaction: DbTransaction):SelectColumnProvider(colindex) {
    override fun match(identificator: String): Boolean {
        if (valueFromExpression.column == null) {
            return false
        }
        return valueFromExpression.column.matches(valueFromExpression.column.tablename, stripSeachName(identificator))
    }

    override fun readValue(selectRowProvider: SelectRowProvider, rowindex: Int): Any? {
        if (valueFromExpression.column == null) {
            return valueFromExpression.valuegen.invoke(Pair(dbTransaction,null))
        }
        return selectRowProvider.readValue(valueFromExpression.column,rowindex)
    }
}