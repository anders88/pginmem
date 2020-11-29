package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import no.anksoft.pginmem.clauses.MatchAllClause
import no.anksoft.pginmem.clauses.WhereClause
import no.anksoft.pginmem.clauses.createWhereClause
import java.sql.ResultSet
import java.sql.SQLException

private fun analyseSelect(words: List<String>, dbStore: DbStore):Pair<List<Table>,WhereClause> {
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
        if (word == "where") {
            break
        }
        usedTables.add(dbStore.tableForRead(word)?:throw SQLException("Unknown table $word"))
    }
    val whereClause:WhereClause = createWhereClause(words.subList(ind,words.size),usedTables)
    return Pair(usedTables,whereClause)
}

class SelectStatement(private val words: List<String>, dbStore: DbStore):DbPreparedStatement() {
    private val pair = analyseSelect(words,dbStore)

    private val tables:List<Table> = pair.first
    private val whereClause:WhereClause = pair.second

    private val colums = tables.map { it.colums }.flatten()

    private val rows:List<List<Cell>> by lazy {
        tables.map { it.rowsForReading() }.flatten().map { it.cells }.filter { whereClause.isMatch(it) }
    }

    override fun executeQuery(): ResultSet {
        val columns = tables.map { it.colums }.flatten()
        return SelectResultSet(columns,rows)
    }

    override fun setString(parameterIndex: Int, x: String?) {
        whereClause.registerBinding(parameterIndex,x)
    }

}