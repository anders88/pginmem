package no.anksoft.pginmem

import no.anksoft.pginmem.clauses.IndexToUse
import no.anksoft.pginmem.statements.ValueGenFromDbCell
import no.anksoft.pginmem.statements.select.ColumnInSelect
import no.anksoft.pginmem.statements.select.SelectColumnProvider
import no.anksoft.pginmem.values.*
import org.jsonbuddy.JsonObject
import org.jsonbuddy.JsonValue
import org.jsonbuddy.parse.JsonParser
import java.math.BigDecimal
import java.sql.SQLException
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

enum class ColumnType(private val altNames:Set<String> = emptySet()) {
    TEXT, TIMESTAMP,DATE,INTEGER(setOf("int","bigint")),BOOLEAN(setOf("bool")),NUMERIC,BYTEA,SERIAL,JSON(setOf("jsonb")),UNSPECIFIED;

    fun validateValue(value:CellValue):CellValue {
        if (value == NullCellValue) {
            return NullCellValue
        }
        val returnValue:CellValue? = when (this) {
            TEXT -> if (value is StringCellValue) value else null
            TIMESTAMP -> when {
                (value is DateTimeCellValue) -> value
                (value is DateCellValue) -> value.valueAsTimestamp()
                else -> null
            }
            DATE -> if (value is DateCellValue) value else null
            INTEGER -> if (value is IntegerCellValue) value else null
            BOOLEAN -> if (value is BooleanCellValue) value else null
            NUMERIC -> when {
                (value is NumericCellValue) -> value
                (value is IntegerCellValue) -> NumericCellValue(value.myValue.toBigDecimal())
                else ->null
            }
            BYTEA -> if (value is ByteArrayCellValue) value else null
            SERIAL -> if (value is IntegerCellValue) value else null
            JSON -> when {
                (value is JsonCellValue) -> value
                (value is StringCellValue) -> JsonCellValue(JsonParser.parseToObject(value.myValue))
                else -> null
            }
            UNSPECIFIED -> throw SQLException("Cannot validate unspecified")
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


    fun convertToMe(cellValue: CellValue):CellValue {
        if (cellValue == NullCellValue) {
            return NullCellValue
        }
        return when(this) {
            TEXT -> cellValue.valueAsText()
            INTEGER -> cellValue.valueAsInteger()
            BOOLEAN -> cellValue.valueAsBoolean()
            JSON -> if (cellValue is StringCellValue) JsonCellValue(JsonParser.parseToObject(cellValue.myValue)) else throw SQLException("Cannot convert to Json")
            else -> throw SQLException("Conversion not supported for ${this.name}")
        }
    }

    fun readFromAny(value:Any?):CellValue {
        if (value == null) {
            return NullCellValue
        }
        return when (this) {
            TEXT -> if (value is String) StringCellValue(value) else TODO()
            TIMESTAMP -> TODO()
            DATE -> TODO()
            INTEGER -> TODO()
            BOOLEAN -> TODO()
            NUMERIC -> TODO()
            BYTEA -> TODO()
            SERIAL -> TODO()
            JSON -> TODO()
            UNSPECIFIED -> TODO()
        }
    }
}

class Column private constructor(override val name:String,val columnType: ColumnType,override val tablename:String,val defaultValue:ValueFromExpression?,val isNotNull:Boolean):ColumnInSelect {



    companion object {
        fun create(selectColumnProvider:SelectColumnProvider,tablename: String):Column =
            Column(
                name = selectColumnProvider.alias?:selectColumnProvider.valueFromExpression.column?.name?:UUID.randomUUID().toString(),
                columnType = ColumnType.UNSPECIFIED,
                tablename = tablename,
                defaultValue = null,
                isNotNull = false
            )


        fun create(tablename:String,statementAnalyzer: StatementAnalyzer,dbTransaction: DbTransaction):Column {
            val columnName:String = statementAnalyzer.word()?:throw SQLException("Expecting column name")
            val colTypeText = statementAnalyzer.addIndex().word()?:throw SQLException("Expecting column type")
            val columnType:ColumnType = ColumnType.values().firstOrNull { it.matchesColumnType(colTypeText) }?:throw SQLException("Unknown column type $colTypeText")

            statementAnalyzer.addIndex(1)

            val defaultValue:ValueFromExpression? = when {
                (columnType == ColumnType.SERIAL) -> {
                    val sequenceName = "${tablename}_${columnName}_seq"
                    dbTransaction.addSequence(sequenceName)
                    SequenceValueFromExpression(sequenceName)
                }
                (statementAnalyzer.word() == "default") -> {
                    statementAnalyzer.addIndex()
                    statementAnalyzer.readValueFromExpression(dbTransaction, emptyMap(), null)
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

    fun setDefault(defvalue:ValueFromExpression?):Column = Column(
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


    override fun matches(tablename: String,name:String):Boolean = ((this.name == name) && (this.tablename == tablename))



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

    override val myValueFromExpression: ValueFromExpression = ValueGenFromDbCell(this)

}