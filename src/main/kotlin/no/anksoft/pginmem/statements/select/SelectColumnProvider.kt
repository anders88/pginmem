package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.Column
import no.anksoft.pginmem.Sequence

abstract class SelectColumnProvider(val colindex:Int) {
    fun isMatch(colindex: Int):Boolean = (this.colindex == colindex)
    abstract fun match(identificator:String):Boolean
    abstract fun readValue(selectRowProvider: SelectRowProvider,rowindex:Int):Any?
}

class SelectDbColumn(private val column: Column,colindex: Int,private val aliasMapping:Map<String,String>):SelectColumnProvider(colindex) {
    override fun match(identificator: String): Boolean {
        val ind = identificator.indexOf(".")
        val tablename:String
        val colname:String
        if (ind == -1) {
            colname = identificator
            tablename = column.tablename
        } else {
            colname = identificator.substring(ind+1)
            val tableOrAlias = identificator.substring(0, ind)
            tablename = aliasMapping[tableOrAlias]?:tableOrAlias
        }
        return column.matches(tablename,colname)
    }
    override fun readValue(selectRowProvider: SelectRowProvider, rowindex: Int): Any? = selectRowProvider.readValue(column,rowindex)
}

class SelectFromSequence(private val sequence: Sequence, colindex:Int):SelectColumnProvider(colindex) {
    override fun match(identificator: String): Boolean = false

    override fun readValue(selectRowProvider: SelectRowProvider, rowindex: Int): Long = sequence.nextVal()

}