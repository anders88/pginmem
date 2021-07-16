package no.anksoft.pginmem

import no.anksoft.pginmem.statements.select.TableInSelect

class Table(override val name:String,defColumns:List<Column>):TableInSelect {
    override val colums:List<Column> = defColumns

    private val rows:MutableList<Row> = mutableListOf()

    override fun findColumn(colname:String):Column? {
        val actualName = stripSeachName(colname)
        return colums.firstOrNull { it.matches(this.name,actualName)}
    }

    fun addRow(row:Row) {
        rows.add(row)
    }

    fun rowsForReading():List<Row> = rows

    override fun rowsFromSelect(dbTransaction: DbTransaction):List<Row> = rowsForReading()

    fun clone():Table {
        val cloned = Table(name,colums)
        rows.forEach { cloned.addRow(it) }
        return cloned
    }

    override fun size():Int = rows.size


}