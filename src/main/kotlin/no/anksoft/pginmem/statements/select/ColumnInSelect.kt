package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.ValueFromExpression

interface ColumnInSelect {
    val myValueFromExpression:ValueFromExpression
    fun matches(tablename: String,name:String):Boolean
    val tablename:String
}