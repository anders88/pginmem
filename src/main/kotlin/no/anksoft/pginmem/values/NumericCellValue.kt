package no.anksoft.pginmem.values

import java.math.BigDecimal
import java.sql.SQLException

class NumericCellValue(val myValue: BigDecimal):CellValue {
    override fun valueAsText(): StringCellValue = StringCellValue(myValue.toString())

    override fun valueAsInteger(): IntegerCellValue = IntegerCellValue(myValue.toLong())

    override fun valueAsBoolean(): BooleanCellValue = BooleanCellValue(myValue == BigDecimal.ZERO)

    override fun valueAsDate(): DateCellValue {
        throw SQLException("Cannot convert numeric to date")
    }

    override fun valueAsTimestamp(): DateTimeCellValue {
        throw SQLException("Cannot convert numeric to timestamp")
    }

    override fun valueAsNumeric(): NumericCellValue = this

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as NumericCellValue

        if (myValue != other.myValue) return false

        return true
    }

    override fun hashCode(): Int {
        return myValue.hashCode()
    }
}