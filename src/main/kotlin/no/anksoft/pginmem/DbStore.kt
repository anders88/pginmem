package no.anksoft.pginmem

import java.sql.SQLException
import java.util.concurrent.ConcurrentHashMap


class DbStore {
    private val tables:MutableMap<String,Table> = mutableMapOf()

    fun createAlterTableSetup(table: Table) {
        tables[table.name] = table
    }

    private val lockedTables:ConcurrentHashMap<String,String> = ConcurrentHashMap()

    fun tableForUpdate(name:String):Table {
        val table = tables[name]?:throw SQLException("Unknown table $name")
        if (lockedTables[table.name] != null) {
            throw NotImplementedError("Wait for lock not implemented")
        }
        lockedTables[table.name] = table.name
        return table.clone()
    }
    fun tableForRead(name:String):Table = tables[name]?:throw SQLException("Unknown table $name")

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



}