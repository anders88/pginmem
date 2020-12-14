package no.anksoft.pginmem

class DbTransaction(private val dbStore: DbStore) {
    private var autoCommit:Boolean = true


    fun createAlterTableSetup(table: Table) {
        dbStore.createAlterTableSetup(table)
    }

    private val updatingTables:MutableMap<String,Table> = mutableMapOf()

    fun tableForUpdate(tablename: String): Table {
        updatingTables[tablename]?.let { return it }
        val table = dbStore.tableForUpdate(tablename)
        updatingTables[table.name] = table
        return table
    }

    fun tableForRead(tablename: String): Table {
        return updatingTables[tablename]?:dbStore.tableForRead(tablename)
    }

    fun registerTableUpdate(table: Table) {
        if (autoCommit) {
            dbStore.updateTableContent(table)
            updatingTables.remove(table.name)
            return
        }
        updatingTables[table.name] = table
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
    }
}