package no.anksoft.pginmem.statements

import no.anksoft.pginmem.DbPreparedStatement
import no.anksoft.pginmem.DbTransaction

class CreateSequenceStatement(words: List<String>, private val dbTransaction: DbTransaction) : DbPreparedStatement() {
    private val name:String = words[2]

    override fun executeUpdate(): Int {
        dbTransaction.addSequence(name)
        return 0
    }
}
