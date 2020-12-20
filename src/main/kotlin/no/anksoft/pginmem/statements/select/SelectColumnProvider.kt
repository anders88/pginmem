package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.Column

abstract class SelectColumnProvider(val colindex:Int) {
    fun isMatch(colindex: Int):Boolean = (this.colindex == colindex)
    abstract fun match(identificator:String):Boolean
    abstract fun readValue(selectRowProvider: SelectRowProvider,rowindex:Int):Any?
}

class SelectDbColumn(private val column: Column,colindex: Int):SelectColumnProvider(colindex) {
    override fun match(identificator: String): Boolean = (column.name == identificator)
    override fun readValue(selectRowProvider: SelectRowProvider, rowindex: Int): Any? = selectRowProvider.readValue(column,rowindex)
}