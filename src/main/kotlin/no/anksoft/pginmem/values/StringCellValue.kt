package no.anksoft.pginmem.values

import java.sql.SQLException
import java.time.LocalDate
import java.time.LocalDateTime

class StringCellValue(val myValue:String):CellValue {
    override fun valueAsText(): StringCellValue = this

    override fun valueAsInteger(): IntegerCellValue = IntegerCellValue(myValue.toLongOrNull()?:throw SQLException("Invalid int $myValue"))

    override fun valueAsBoolean(): BooleanCellValue = when (myValue.toLowerCase()) {
        "true" -> BooleanCellValue(true)
        "false" -> BooleanCellValue(false)
        else -> throw SQLException("Cannot convert to boolean $myValue")
    }

    override fun valueAsDate(): DateCellValue = DateCellValue(LocalDate.parse(myValue))

    override fun valueAsTimestamp(): DateTimeCellValue = DateTimeCellValue(LocalDateTime.parse(myValue))

    override fun valueAsNumeric(): NumericCellValue = NumericCellValue(myValue.toBigDecimalOrNull()?:throw SQLException("Invalid numeric $myValue"))

    override fun compareMeTo(other: CellValue, nullsFirst: Boolean): Int = when {
        this == other -> 0
        other == NullCellValue -> if (nullsFirst) 1 else -1
        else -> this.myValue.compareTo(other.valueAsText().myValue)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as StringCellValue

        if (myValue != other.myValue) return false

        return true
    }

    override fun hashCode(): Int {
        return myValue.hashCode()
    }

    override fun toString(): String {
        return "StringCellValue(myValue='$myValue')"
    }


}