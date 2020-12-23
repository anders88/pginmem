package no.anksoft.pginmem.statements

import no.anksoft.pginmem.DbPreparedStatement
import no.anksoft.pginmem.DbTransaction
import no.anksoft.pginmem.StatementAnalyzer
import java.sql.SQLException

class CreateSequenceStatement(statementAnalyzer: StatementAnalyzer, private val dbTransaction: DbTransaction) : DbPreparedStatement() {
    private val name:String = statementAnalyzer.word(2)?:throw SQLException("Expected sequence name")

    override fun executeUpdate(): Int {
        dbTransaction.addSequence(name)
        return 0
    }
}
