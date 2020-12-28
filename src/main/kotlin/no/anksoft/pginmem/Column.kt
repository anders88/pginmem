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

class Column private constructor(val name:String,val columnType: ColumnType,val defaultValue:((DbTransaction)->Any?)?,val isNotNull:Boolean) {


    companion object {
        fun create(statementAnalyzer: StatementAnalyzer,dbTransaction: DbTransaction):Column {
            val columnName = statementAnalyzer.word()?:throw SQLException("Expecting column name")
            val colTypeText = statementAnalyzer.addIndex().word()?:throw SQLException("Expecting column type")
            val columnType:ColumnType = ColumnType.values().firstOrNull { it.name.toLowerCase() == colTypeText }?:throw SQLException("Unknown column type $colTypeText")

            statementAnalyzer.addIndex(1)

            val defaultValue:((DbTransaction)->Any?)? = if (statementAnalyzer.word() == "default") {
                statementAnalyzer.addIndex()
                statementAnalyzer.readConstantValue(dbTransaction)
            } else null
            val isNotNull = if (statementAnalyzer.word() == "not" && statementAnalyzer.word(1) == "null") {
                statementAnalyzer.addIndex(2)
                true
            } else false
            return Column(columnName,columnType,defaultValue,isNotNull)
        }
    }

    fun rename(newname:String):Column = Column(
        name = newname,
        columnType = columnType,
        defaultValue = defaultValue,
        isNotNull = isNotNull
    )





    override fun equals(other: Any?): Boolean {
        if (other !is Column) return false
        return other.name == name
    }

    override fun hashCode(): Int {
        return name.hashCode()
    }
}