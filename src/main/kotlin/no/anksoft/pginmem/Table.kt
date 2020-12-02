package no.anksoft.pginmem

class Table(val name:String,defColumns:List<Column>) {
    val colums:List<Column> = defColumns



    private val rows:MutableList<Row> = mutableListOf()

    fun findColumn(name:String):Column? = colums.firstOrNull { it.name == name }

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