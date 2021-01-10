package no.anksoft.pginmem.values

import java.math.BigDecimal
import java.sql.SQLException

class BooleanCellValue(val myValue:Boolean):CellValue {
    override fun valueAsText(): StringCellValue = StringCellValue(myValue.toString())

    override fun valueAsInteger(): IntegerCellValue = IntegerCellValue(if (myValue) 1L else 0L)

    override fun valueAsBoolean(): BooleanCellValue = this

    override fun valueAsDate(): DateCellValue {
        throw SQLException("Cannot read boolean as date")
    }

    override fun valueAsTimestamp(): DateTimeCellValue {
        throw SQLException("Cannot read boolean as timestamp")
    }

    override fun valueAsNumeric(): NumericCellValue = NumericCellValue(if (myValue) BigDecimal.ONE else BigDecimal.ZERO)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as BooleanCellValue

        if (myValue != other.myValue) return false

        return true
    }

    override fun hashCode(): Int {
        return myValue.hashCode()
    }
}