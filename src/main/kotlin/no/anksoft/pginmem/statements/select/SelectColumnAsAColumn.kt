package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.ValueFromExpression

class SelectColumnAsAColumn(private val scp:SelectColumnProvider,ownerTableName:String):ColumnInSelect {


    override val tablename: String = scp.alias?:ownerTableName
    override val name: String? = scp.valueFromExpression.column?.name

    override val myValueFromExpression: ValueFromExpression = scp.valueFromExpression

    override fun matches(givenTablename: String, name: String): Boolean = scp.isMatch(name)


}