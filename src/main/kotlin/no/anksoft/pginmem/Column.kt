package no.anksoft.pginmem

import java.sql.SQLException
import java.sql.Timestamp

enum class ColumnType() {
    TEXT, TIMESTAMP;

    fun validateValue(value:Any?) {
        if (value == null) {
            return
        }
        val ok:Boolean = when (this) {
            TEXT -> (value is String)
            TIMESTAMP -> (value is Timestamp)
        }
        if (!ok) {
            throw SQLException("Binding value not valid for $this")
        }
    }
}

class Column constructor(val name:String,columnTypeText:String) {
    val columnType:ColumnType = ColumnType.values().firstOrNull { it.name.toLowerCase() == columnTypeText }?:throw SQLException("Unknown column type $columnTypeText")


}