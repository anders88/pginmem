package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import no.anksoft.pginmem.clauses.MatchAllClause
import no.anksoft.pginmem.clauses.WhereClause
import no.anksoft.pginmem.clauses.createWhereClause
import java.sql.ResultSet
import java.sql.SQLException

private fun analyseSelect(words: List<String>, dbTransaction: DbTransaction,sql:String):Pair<List<Table>,WhereClause> {
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
        usedTables.add(dbTransaction.tableForRead(word)?:throw SQLException("Unknown table $word"))
    }
    val whereClause:WhereClause = if (ind < words.size) createWhereClause(words.subList(ind,words.size),usedTables,1) else MatchAllClause()
    return Pair(usedTables,whereClause)
}

class SelectStatement(words: List<String>, dbTransaction: DbTransaction,private val sqlOrig:String):DbPreparedStatement() {
    private val pair = analyseSelect(words,dbTransaction,sqlOrig)

    private val tables:List<Table> = pair.first
    private val whereClause:WhereClause = pair.second


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

    override fun setInt(parameterIndex: Int, x: Int) {
        whereClause.registerBinding(parameterIndex,x)
    }

}