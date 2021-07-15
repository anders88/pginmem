package no.anksoft.pginmem.statements

import no.anksoft.pginmem.DbPreparedStatement
import java.sql.ResultSet


class StatementToReturnFixed(val rows:List<List<Pair<String,Any?>>>,private val sql:String):DbPreparedStatement(null) {
    override fun executeQuery(): ResultSet {
        return ResultSetFromRows(rows,sql)
    }

    override fun execute(): Boolean {
        return false
    }
}