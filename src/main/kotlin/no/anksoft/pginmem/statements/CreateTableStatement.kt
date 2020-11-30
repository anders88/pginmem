package no.anksoft.pginmem.statements

import no.anksoft.pginmem.Column
import no.anksoft.pginmem.DbPreparedStatement
import no.anksoft.pginmem.DbStore
import no.anksoft.pginmem.Table

class CreateTableStatement(val words:List<String>,val dbStore: DbStore):DbPreparedStatement() {

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
        dbStore.addTable(table)
        return 0
    }
}