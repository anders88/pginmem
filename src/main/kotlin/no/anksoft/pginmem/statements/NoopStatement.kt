package no.anksoft.pginmem.statements

import no.anksoft.pginmem.DbPreparedStatement

class NoopStatement:DbPreparedStatement() {
    override fun execute(): Boolean {
        return false
    }

    override fun close() {

    }

    override fun setString(parameterIndex: Int, x: String?) {

    }

    override fun executeUpdate(): Int {
        return 0
    }
}