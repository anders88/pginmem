package no.anksoft.pginmem

import java.math.BigDecimal
import java.sql.SQLException
import java.sql.Timestamp

enum class ColumnType() {
    TEXT, TIMESTAMP,DATE,INTEGER,BOOLEAN,NUMERIC;

    fun validateValue(value:Any?):Any? {
        if (value == null) {
            return null
        }
        val returnValue:Any? = when (this) {
            TEXT -> if (value is String) value else null
            TIMESTAMP -> if (value is Timestamp) value else null
            DATE -> if (value is Timestamp) value else null
            INTEGER -> if (value is Number) value.toLong() else null
            BOOLEAN -> if (value is Boolean) value else null
            NUMERIC -> if (value is Number) BigDecimal.valueOf(value.toDouble()) else null
        }
        if (returnValue == null) {
            throw SQLException("Binding value not valid for $this")
        }
        return returnValue
    }
}

class Column constructor(val name:String,columnTypeText:String,val defaultValue:(()->Any?)?,val isNotNull:Boolean) {
    val columnType:ColumnType = ColumnType.values().firstOrNull { it.name.toLowerCase() == columnTypeText }?:throw SQLException("Unknown column type $columnTypeText")

    override fun equals(other: Any?): Boolean {
        if (other !is Column) return false
        return other.name == name
    }

    override fun hashCode(): Int {
        return name.hashCode()
    }
}