package no.anksoft.pginmem

import no.anksoft.pginmem.values.*
import java.math.BigDecimal
import java.sql.SQLException
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

enum class ColumnType(private val altNames:Set<String> = emptySet()) {
    TEXT, TIMESTAMP,DATE,INTEGER(setOf("int","bigint")),BOOLEAN(setOf("bool")),NUMERIC,BYTEA,SERIAL;

    fun validateValue(value:CellValue):CellValue {
        if (value == NullCellValue) {
            return NullCellValue
        }
        val returnValue:CellValue? = when (this) {
            TEXT -> if (value is StringCellValue) value else null
            TIMESTAMP -> if (value is DateTimeCellValue) value else null
            DATE -> if (value is DateCellValue) value else null
            INTEGER -> if (value is IntegerCellValue) value else null
            BOOLEAN -> if (value is BooleanCellValue) value else null
            NUMERIC -> if (value is NumericCellValue) value else null
            BYTEA -> if (value is ByteArrayCellValue) value else null
            SERIAL -> if (value is IntegerCellValue) value else null
        }
        if (returnValue == null) {
            throw SQLException("Binding value not valid for $this")
        }
        return returnValue
    }

    fun matchesColumnType(colTypeText:String):Boolean {
        if (this.name.toLowerCase() == colTypeText) return true
        return this.altNames.contains(colTypeText)
    }

    fun convertValue(to:ColumnType,value:Any?):Any? {
        if (value == null) {
            return null
        }
        return when (this) {
            TEXT -> when(to) {
                TEXT -> value
                TIMESTAMP -> LocalDateTime.parse(value.toString())
                DATE -> LocalDate.parse(value.toString()).atStartOfDay()
                INTEGER -> value.toString().toLong()
                BOOLEAN -> if (value == true) true else false
                NUMERIC -> BigDecimal.valueOf(value.toString().toDouble())
                BYTEA -> throw SQLException("Bytea conversion not supported")
                SERIAL -> throw SQLException("Serial conversion not supported")
            }
            TIMESTAMP -> TODO()
            DATE -> TODO()
            INTEGER -> TODO()
            BOOLEAN -> return when(to) {
                TEXT -> return value.toString()
                TIMESTAMP -> TODO()
                DATE -> TODO()
                INTEGER -> if (value == true) 1 else 0
                BOOLEAN -> value
                NUMERIC -> if (value == true) BigDecimal.ONE else BigDecimal.ZERO
                BYTEA -> TODO()
                SERIAL -> TODO()
            }
            NUMERIC -> TODO()
            BYTEA -> throw SQLException("Bytea conversion not supported")
            SERIAL -> throw SQLException("Serial conversion not supported")
            NUMERIC -> TODO()
        }
    }

    fun convertToMe(cellValue: CellValue):CellValue {
        if (cellValue == NullCellValue) {
            return NullCellValue
        }
        return when(this) {
            TEXT -> cellValue.valueAsText()
            INTEGER -> cellValue.valueAsInteger()
            BOOLEAN -> cellValue.valueAsBoolean()
            else -> throw SQLException("Conversion not supported for ${this.name}")
        }
    }
}

class Column private constructor(val name:String,val columnType: ColumnType,val tablename:String,val defaultValue:((Pair<DbTransaction,Row?>)->CellValue)?,val isNotNull:Boolean) {


    companion object {
        fun create(tablename:String,statementAnalyzer: StatementAnalyzer,dbTransaction: DbTransaction):Column {
            val columnName:String = statementAnalyzer.word()?:throw SQLException("Expecting column name")
            val colTypeText = statementAnalyzer.addIndex().word()?:throw SQLException("Expecting column type")
            val columnType:ColumnType = ColumnType.values().firstOrNull { it.matchesColumnType(colTypeText) }?:throw SQLException("Unknown column type $colTypeText")

            statementAnalyzer.addIndex(1)

            val defaultValue:((Pair<DbTransaction,Row?>)->CellValue)? = when {
                (columnType == ColumnType.SERIAL) -> {
                    val sequenceName = "${tablename}_${columnName}_seq"
                    dbTransaction.addSequence(sequenceName)
                    val d: (Pair<DbTransaction,Row?>) -> CellValue = { it.first.sequence(sequenceName).nextVal() }
                    d
                }
                (statementAnalyzer.word() == "default") -> {
                    statementAnalyzer.addIndex()
                    statementAnalyzer.readValueFromExpression(dbTransaction, emptyMap()).valuegen
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

    fun setDefault(defvalue:((Pair<DbTransaction,Row?>)->CellValue)?):Column = Column(
        name = name,
        columnType = columnType,
        tablename = tablename,
        defaultValue = defvalue,
        isNotNull = isNotNull
    )

    fun changeColumnType(newColumnType: ColumnType):Column = Column(
        name = name,
        columnType = newColumnType,
        tablename = tablename,
        defaultValue = null,
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