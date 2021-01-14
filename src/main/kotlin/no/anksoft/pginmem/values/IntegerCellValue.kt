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

    override fun compareMeTo(other: CellValue, nullsFirst: Boolean): Int = when {
        this == other -> 0
        other == NullCellValue -> if (nullsFirst) 1 else -1
        other is IntegerCellValue -> this.myValue.compareTo(other.myValue)
        other is NumericCellValue -> this.myValue.toBigDecimal().compareTo(other.myValue)
        else -> throw SQLException("Cannot compare $this and $other")
    }

    override fun add(cellValue: CellValue): CellValue = when {
        cellValue == NullCellValue -> this
        cellValue is IntegerCellValue -> IntegerCellValue(this.myValue+cellValue.myValue)
        cellValue is NumericCellValue -> NumericCellValue(this.myValue.toBigDecimal().add(cellValue.myValue))
        else -> throw SQLException("Cannot add $cellValue to int")
    }


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

    override fun toString(): String {
        return "IntegerCellValue(myValue=$myValue)"
    }
}