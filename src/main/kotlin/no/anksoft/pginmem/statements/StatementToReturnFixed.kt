package no.anksoft.pginmem.statements

import no.anksoft.pginmem.DbPreparedStatement
import java.sql.ResultSet


class StatementToReturnFixed(val rows:List<List<Pair<String,Any?>>>):DbPreparedStatement() {
    override fun executeQuery(): ResultSet {
        return ResultSetFromRows(rows)
    }
}