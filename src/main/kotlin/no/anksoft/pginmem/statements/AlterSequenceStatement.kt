package no.anksoft.pginmem.statements

import no.anksoft.pginmem.DbPreparedStatement
import no.anksoft.pginmem.DbTransaction
import no.anksoft.pginmem.StatementAnalyzer
import java.sql.SQLException

class AlterSequenceStatement(val statementAnalyzer: StatementAnalyzer, private val dbTransaction: DbTransaction) : DbPreparedStatement(dbTransaction) {

    override fun executeUpdate(): Int {
        val name:String = statementAnalyzer.addIndex(2).word()?:throw SQLException("Expected sequence name")
        if (statementAnalyzer.addIndex().word() != "restart") {
            throw SQLException("Expected restart in alter sequence")
        }
        val sequence = dbTransaction.sequence(name)
        dbTransaction.resetSequence(sequence)
        return 0
    }
}
