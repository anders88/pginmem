package no.anksoft.pginmem

class Table(val name:String,defColumns:List<Column>) {
    val colums:List<Column> = defColumns

    private val rows:MutableList<Row> = mutableListOf()

    fun findColumn(colname:String):Column? {
        val actualName = stripSeachName(colname)
        return colums.firstOrNull { it.matches(this.name,actualName)}
    }

    fun addRow(row:Row) {
        rows.add(row)
    }

    fun rowsForReading():List<Row> = rows

    fun clone():Table {
        val cloned = Table(name,colums)
        rows.forEach { cloned.addRow(it) }
        return cloned
    }


}