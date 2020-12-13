package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import java.sql.SQLException
import java.sql.Timestamp
import java.time.LocalDateTime

class CreateTableStatement(val words:List<String>,private val dbTransaction: DbTransaction):DbPreparedStatement() {

    override fun executeUpdate(): Int {
        val name = words[2]
        var ind = 4
        val columns:MutableList<Column> = mutableListOf()
        while (true) {
            val columnName = words[ind]
            val colType = words[ind+1]
            ind+=2
            val defaultValue:(()->Any?)? = if (words[ind] == "default") {
                ind++
                if (words[ind] == "now" && words[ind+1] == "(" && words[ind+2] == ")") {
                    ind+=3
                    { Timestamp.valueOf(LocalDateTime.now()) }
                } else throw SQLException("Unknown default value for column $columnName")

            } else null
            columns.add(Column(columnName,colType,defaultValue))
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