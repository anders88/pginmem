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
}