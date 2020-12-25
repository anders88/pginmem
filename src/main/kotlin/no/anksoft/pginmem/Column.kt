package no.anksoft.pginmem

import java.math.BigDecimal
import java.sql.SQLException
import java.sql.Timestamp

enum class ColumnType() {
    TEXT, TIMESTAMP,DATE,INTEGER,BOOLEAN,NUMERIC,BYTEA;

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
            BYTEA -> if (value is ByteArray) value else null
        }
        if (returnValue == null) {
            throw SQLException("Binding value not valid for $this")
        }
        return returnValue
    }
}

class Column private constructor(val name:String,columnTypeText:String,val defaultValue:(()->Any?)?,val isNotNull:Boolean) {
    companion object {
        fun create(statementAnalyzer: StatementAnalyzer):Column {
            val columnName = statementAnalyzer.word()?:throw SQLException("Expecting column name")
            val colType = statementAnalyzer.word(1)?:throw SQLException("Expecting column type")
            statementAnalyzer.addIndex(2)

            val defaultValue:(()->Any?)? = if (statementAnalyzer.word() == "default") {
                statementAnalyzer.addIndex()
                statementAnalyzer.readConstantValue()
            } else null
            val isNotNull = if (statementAnalyzer.word() == "not" && statementAnalyzer.word(1) == "null") {
                statementAnalyzer.addIndex(2)
                true
            } else false
            return Column(columnName,colType,defaultValue,isNotNull)
        }
    }


    val columnType:ColumnType = ColumnType.values().firstOrNull { it.name.toLowerCase() == columnTypeText }?:throw SQLException("Unknown column type $columnTypeText")



    override fun equals(other: Any?): Boolean {
        if (other !is Column) return false
        return other.name == name
    }

    override fun hashCode(): Int {
        return name.hashCode()
    }
}