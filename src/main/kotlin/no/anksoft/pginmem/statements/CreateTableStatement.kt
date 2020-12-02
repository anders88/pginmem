package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*

class CreateTableStatement(val words:List<String>,private val dbTransaction: DbTransaction):DbPreparedStatement() {

    override fun executeUpdate(): Int {
        val name = words[2]
        var ind = 4
        val columns:MutableList<Column> = mutableListOf()
        while (true) {
            columns.add(Column(words[ind],words[ind+1]))
            ind+=2
            if (words[ind] == ")") {
                break
            }
            ind++
        }
        val table = Table(name,columns)
        dbTransaction.createAlterTableSetup(table)
        return 0
    }
}