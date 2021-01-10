package no.anksoft.pginmem.values

import java.sql.SQLException
import java.time.LocalDate

class DateCellValue(val myValue:LocalDate):CellValue {
    override fun valueAsText(): StringCellValue = StringCellValue(myValue.toString())

    override fun valueAsInteger(): IntegerCellValue {
        throw SQLException("Cannot get date as integer")
    }

    override fun valueAsBoolean(): BooleanCellValue {
        throw SQLException("Cannot get date as boolean")
    }

    override fun valueAsDate(): DateCellValue = this


    override fun valueAsTimestamp(): DateTimeCellValue = DateTimeCellValue(myValue.atStartOfDay())

    override fun valueAsNumeric(): NumericCellValue {
        throw SQLException("Cannot get date as numeric")
    }

    override fun compareMeTo(other: CellValue, nullsFirst: Boolean): Int = when {
        this == other -> 0
        other == NullCellValue -> if (nullsFirst) 1 else -1
        other is DateCellValue -> this.myValue.compareTo(other.myValue)
        other is DateTimeCellValue -> this.myValue.atStartOfDay().compareTo(other.myValue)
        else -> throw SQLException("Cannot compare $this and $other")
    }


    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as DateCellValue

        if (myValue != other.myValue) return false

        return true
    }

    override fun hashCode(): Int {
        return myValue.hashCode()
    }

    override fun toString(): String {
        return "DateCellValue(myValue=$myValue)"
    }
}