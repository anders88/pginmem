package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.Column
import no.anksoft.pginmem.Sequence
import no.anksoft.pginmem.values.CellValue

abstract class SelectColumnProvider(val colindex:Int) {
    fun isMatch(colindex: Int):Boolean = (this.colindex == colindex)
    abstract fun match(identificator:String):Boolean
    abstract fun readValue(selectRowProvider: SelectRowProvider,rowindex:Int):CellValue
}

class SelectDbColumn constructor(private val column: Column,colindex: Int,private val aliasMapping:Map<String,String>):SelectColumnProvider(colindex) {
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
    override fun readValue(selectRowProvider: SelectRowProvider, rowindex: Int): CellValue = selectRowProvider.readValue(column,rowindex)
}

class SelectColumnWithAlias(colindex: Int,val aliasFor:SelectColumnProvider, val alias:String):SelectColumnProvider(colindex) {
    override fun match(identificator: String): Boolean {
        return (alias == identificator)
    }

    override fun readValue(selectRowProvider: SelectRowProvider, rowindex: Int): CellValue {
        return aliasFor.readValue(selectRowProvider,rowindex)
    }
}

class SelectFromSequence(private val sequence: Sequence, colindex:Int):SelectColumnProvider(colindex) {
    override fun match(identificator: String): Boolean = false

    override fun readValue(selectRowProvider: SelectRowProvider, rowindex: Int): CellValue = sequence.nextVal()

}