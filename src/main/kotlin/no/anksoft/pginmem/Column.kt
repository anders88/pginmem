package no.anksoft.pginmem

import java.math.BigDecimal
import java.sql.SQLException
import java.sql.Timestamp
import java.util.*

enum class ColumnType() {
    TEXT, TIMESTAMP,DATE,INTEGER,BOOLEAN,NUMERIC,BYTEA,SERIAL;

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
            SERIAL -> if (value is Number) value.toLong() else null
        }
        if (returnValue == null) {
            throw SQLException("Binding value not valid for $this")
        }
        return returnValue
    }
}

class Column private constructor(private val name:String,val columnType: ColumnType,val tablename:String,val defaultValue:((Pair<DbTransaction,Row?>)->Any?)?,val isNotNull:Boolean) {


    companion object {
        fun create(tablename:String,statementAnalyzer: StatementAnalyzer,dbTransaction: DbTransaction):Column {
            val columnName = statementAnalyzer.word()?:throw SQLException("Expecting column name")
            val colTypeText = statementAnalyzer.addIndex().word()?:throw SQLException("Expecting column type")
            val columnType:ColumnType = ColumnType.values().firstOrNull { it.name.toLowerCase() == colTypeText }?:throw SQLException("Unknown column type $colTypeText")

            statementAnalyzer.addIndex(1)

            val defaultValue:((Pair<DbTransaction,Row?>)->Any?)? = when {
                (columnType == ColumnType.SERIAL) -> {
                    val sequenceName = "${tablename}_${columnName}_seq"
                    dbTransaction.addSequence(sequenceName)
                    val d: (Pair<DbTransaction,Row?>) -> Any? = { it.first.sequence(sequenceName).nextVal() }
                    d
                }
                (statementAnalyzer.word() == "default") -> {
                    statementAnalyzer.addIndex()
                    statementAnalyzer.readValueFromExpression(dbTransaction, emptyList())
                }
                else -> null
            }

            val isNotNull = if (statementAnalyzer.word() == "not" && statementAnalyzer.word(1) == "null") {
                statementAnalyzer.addIndex(2)
                true
            } else false
            return Column(columnName,columnType,tablename,defaultValue,isNotNull)
        }
    }

    fun rename(newname:String):Column = Column(
        name = newname,
        columnType = columnType,
        tablename = tablename,
        defaultValue = defaultValue,
        isNotNull = isNotNull
    )

    fun renameTable(newTableName:String):Column = Column(
        name = name,
        columnType = columnType,
        tablename = newTableName,
        defaultValue = defaultValue,
        isNotNull = isNotNull
    )


    fun matches(tablename: String,name:String):Boolean = ((this.name == name) && (this.tablename == tablename))



    override fun equals(other: Any?): Boolean {
        if (other !is Column) return false
        return (other.name == name && other.tablename == tablename)
    }

    override fun hashCode(): Int {
        return Objects.hash(name,tablename)
    }

    override fun toString(): String {
        return "Column(name='$name', columnType=$columnType)"
    }


}