package no.anksoft.pginmem.statements

import no.anksoft.pginmem.DbPreparedStatement
import no.anksoft.pginmem.DbTransaction

class CreateIndexStatement(val dbTransaction: DbTransaction):DbPreparedStatement(dbTransaction) {
    override fun executeUpdate(): Int {
        return 0
    }

    override fun close() {
        TODO()
    }
}