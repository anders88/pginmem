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

    override fun compareMeTo(other: CellValue): Int = when {
        this == other -> 0
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
        return when {
            this === other -> true
            (other is IntegerCellValue) -> (this.myValue == other.myValue)
            (other is NumericCellValue) -> (BigDecimal.valueOf(this.myValue).compareTo(other.myValue) == 0)
            else -> false
        }
    }

    override fun hashCode(): Int {
        return myValue.hashCode()
    }

    override fun toString(): String {
        return "IntegerCellValue(myValue=$myValue)"
    }
}