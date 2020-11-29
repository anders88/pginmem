package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import java.sql.ResultSet
import java.sql.SQLException

private fun readTables(words: List<String>, dbStore: DbStore):List<Table> {
    val usedTables = mutableListOf<Table>()
    var picking = false
    var ind = 0;
    while (ind < words.size) {
        val word = words[ind]
        ind++
        if (!picking) {
            if (word == "from") {
                picking = true
            }
            continue
        }
        usedTables.add(dbStore.tableForRead(word)?:throw SQLException(""))
    }
    return usedTables
}

class SelectStatement(private val words: List<String>, dbStore: DbStore):DbPreparedStatement() {
    private val tables:List<Table> = readTables(words,dbStore)
    private val rows:List<List<Cell>> by lazy {
        tables.map { it.rowsForReading() }.flatten().map { it.cells }
    }

    override fun executeQuery(): ResultSet {
        val columns = tables.map { it.colums }.flatten()
        return SelectResultSet(columns,rows)
    }

}