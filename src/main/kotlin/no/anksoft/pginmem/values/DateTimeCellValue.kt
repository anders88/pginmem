package no.anksoft.pginmem.values

import java.sql.SQLException
import java.time.LocalDateTime

class DateTimeCellValue(val myValue:LocalDateTime):CellValue {
    override fun valueAsText(): StringCellValue = StringCellValue(myValue.toString())

    override fun valueAsInteger(): IntegerCellValue {
        throw SQLException("Cannot get timestamp as integer")
    }

    override fun valueAsBoolean(): BooleanCellValue {
        throw SQLException("Cannot get timestamp as boolean")
    }

    override fun valueAsDate(): DateCellValue = DateCellValue(myValue.toLocalDate())

    override fun valueAsTimestamp(): DateTimeCellValue = this

    override fun valueAsNumeric(): NumericCellValue {
        throw SQLException("Cannot get timestamp as numeric")
    }

    override fun compareMeTo(other: CellValue, nullsFirst: Boolean): Int = when {
        this == other -> 0
        other == NullCellValue -> if (nullsFirst) 1 else -1
        other is DateCellValue -> this.myValue.compareTo(other.myValue.atStartOfDay())
        other is DateTimeCellValue -> this.myValue.compareTo(other.myValue)
        else -> throw SQLException("Cannot compare $this and $other")
    }




    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as DateTimeCellValue

        if (myValue != other.myValue) return false

        return true
    }

    override fun hashCode(): Int {
        return myValue.hashCode()
    }

    override fun toString(): String {
        return "DateTimeCellValue(myValue=$myValue)"
    }
}