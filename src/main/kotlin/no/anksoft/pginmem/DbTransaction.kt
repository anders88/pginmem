package no.anksoft.pginmem

import java.sql.SQLException

class DbTransaction constructor(private val dbStore: DbStore) {
    private var autoCommit:Boolean = true
    private var isClosed:Boolean = false


    fun createAlterTableSetup(table: Table) {
        dbStore.createAlterTableSetup(table)
    }

    private val updatingTables:MutableMap<String,Table> = mutableMapOf()

    fun tableForUpdate(tablename: String): Table = tableForUpdateIdOrNull(tablename)?:throw SQLException("Unknown table $tablename")

    fun tableForUpdateIdOrNull(tablename: String) :Table? {
        updatingTables[tablename]?.let { return it }
        val table = dbStore.tableForUpdate(tablename)?:return null
        updatingTables[table.name] = table
        return table
    }

    fun tableForRead(tablename: String): Table {
        return updatingTables[tablename]?:dbStore.tableForRead(tablename)
    }

    fun doesTableExsist(tablename:String):Boolean = dbStore.doesTableExsist(tablename)


    fun registerTableUpdate(table: Table) {
        if (autoCommit) {
            dbStore.updateTableContent(table)
            updatingTables.remove(table.name)
            return
        }
        updatingTables[table.name] = table
    }

    fun removeTable(table: Table) {
        dbStore.removeTable(table)
        updatingTables.remove(table.name)
    }

    fun setAutoCommit(value: Boolean) {
        this.autoCommit = value
    }

    fun isAutoCommit():Boolean = autoCommit

    fun rollback() {
        updatingTables.forEach { dbStore.releaseLock(it.key)}
        updatingTables.clear()
    }

    fun commit() {
        updatingTables.forEach { dbStore.updateTableContent(it.value)}
        updatingTables.clear()
    }

    fun close() {
        rollback()
        isClosed = true
    }

    fun sequence(name: String):Sequence = dbStore.sequence(name)

    fun addSequence(name: String) = dbStore.addSequence(name)

    fun resetSequence(sequence: Sequence) = dbStore.resetSequence(sequence)

    fun isClosed():Boolean = isClosed



}