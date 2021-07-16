package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.Column
import no.anksoft.pginmem.DbTransaction
import no.anksoft.pginmem.Row

interface TableInSelect {
    val name:String
    val colums:List<ColumnInSelect>

    fun findColumn(colname:String): ColumnInSelect?
    fun rowsFromSelect(dbTransaction: DbTransaction):List<Row>
    fun size():Int
}