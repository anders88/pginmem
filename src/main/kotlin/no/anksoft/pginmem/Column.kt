package no.anksoft.pginmem

import java.sql.SQLException
import java.sql.Timestamp

enum class ColumnType() {
    TEXT, TIMESTAMP,INTEGER,BOOLEAN;

    fun validateValue(value:Any?) {
        if (value == null) {
            return
        }
        val ok:Boolean = when (this) {
            TEXT -> (value is String)
            TIMESTAMP -> (value is Timestamp)
            INTEGER -> (value is Int)
            BOOLEAN -> (value is Boolean)
        }
        if (!ok) {
            throw SQLException("Binding value not valid for $this")
        }
    }
}

class Column constructor(val name:String,columnTypeText:String,val defaultValue:(()->Any?)?) {
    val columnType:ColumnType = ColumnType.values().firstOrNull { it.name.toLowerCase() == columnTypeText }?:throw SQLException("Unknown column type $columnTypeText")

    override fun equals(other: Any?): Boolean {
        if (other !is Column) return false
        return other.name == name
    }

    override fun hashCode(): Int {
        return name.hashCode()
    }
}