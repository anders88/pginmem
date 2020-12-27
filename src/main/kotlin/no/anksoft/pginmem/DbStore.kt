package no.anksoft.pginmem

import java.sql.SQLException
import java.util.concurrent.ConcurrentHashMap


fun stripSeachName(searchName:String):String {
    var actualName:String = searchName
    actualName = actualName.indexOf(".").let { if (it == -1) actualName else actualName.substring(it+1) }
    if (actualName.startsWith("\"")) {
        actualName = actualName.substring(1)
    }
    if (actualName.endsWith("\"")) {
        actualName = actualName.substring(0,actualName.length-1)
    }
    return actualName
}

class DbStore {
    private val tables:MutableMap<String,Table> = mutableMapOf()
    private val sequences:MutableMap<String,Sequence> = mutableMapOf()

    fun createAlterTableSetup(table: Table) {
        tables[table.name] = table
    }

    private val lockedTables:ConcurrentHashMap<String,String> = ConcurrentHashMap()

    private fun findTable(tablename:String):Table {
        val table = tables[stripSeachName(tablename)]?:throw SQLException("Unknown table $tablename")
        return table
    }

    fun tableForUpdate(name:String):Table {
        val table = findTable(name)
        if (lockedTables[table.name] != null) {
            throw NotImplementedError("Wait for lock not implemented for table ${table.name}")
        }
        lockedTables[table.name] = table.name
        return table.clone()
    }
    fun tableForRead(name:String):Table = findTable(name)

    fun createConnection():DbConnection {
        return DbConnection(DbTransaction(this))
    }

    fun updateTableContent(table: Table) {
        tables[table.name] = table
        lockedTables.remove(table.name)
    }

    fun releaseLock(tablename:String) {
        lockedTables.remove(tablename)
    }

    fun sequence(name: String):Sequence = sequences[name]?:throw SQLException("Unknown sequence $name")

    fun addSequence(name: String) {
        if (sequences[name] != null) {
            throw SQLException("Sequence $name already exsists")
        }
        sequences[name] = Sequence(name)
    }

}