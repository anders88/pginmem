package no.anksoft.pginmem

class Table(val name:String,defColumns:List<Column>) {
    val colums:List<Column> = defColumns



    private val rows:MutableList<Row> = mutableListOf()

    fun findColumn(name:String):Column? = colums.firstOrNull { it.name == name }

    fun addRow(row:Row) {
        rows.add(row)
    }


}