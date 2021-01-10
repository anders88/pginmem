package no.anksoft.pginmem.values

import java.math.BigDecimal
import java.sql.SQLException

class IntegerCellValue(val myValue:Long):CellValue {
    override fun valueAsText(): StringCellValue = StringCellValue(myValue.toString())

    override fun valueAsInteger(): IntegerCellValue = this

    override fun valueAsBoolean(): BooleanCellValue = BooleanCellValue(myValue == 0L)

    override fun valueAsDate(): DateCellValue {
        throw SQLException("Cannot get number as date")
    }

    override fun valueAsTimestamp(): DateTimeCellValue {
        throw SQLException("Cannot get number as timestamp")

    }

    override fun valueAsNumeric(): NumericCellValue = NumericCellValue(BigDecimal.valueOf(myValue))

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as IntegerCellValue

        if (myValue != other.myValue) return false

        return true
    }

    override fun hashCode(): Int {
        return myValue.hashCode()
    }
}