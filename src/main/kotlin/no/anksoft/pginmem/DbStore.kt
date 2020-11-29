package no.anksoft.pginmem



class DbStore {
    private val tables:MutableMap<String,Table> = mutableMapOf()

    fun addTable(table: Table) {
        tables[table.name] = table
    }

    fun tableForUpdate(name:String):Table? = tables[name]
    fun tableForRead(name:String):Table? = tables[name]


}