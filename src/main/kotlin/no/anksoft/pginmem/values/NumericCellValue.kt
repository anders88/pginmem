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

    override fun compareMeTo(other: CellValue): Int = when {
        this == other -> 0
        other is IntegerCellValue -> this.myValue.compareTo(other.myValue.toBigDecimal())
        other is NumericCellValue -> this.myValue.compareTo(other.myValue)
        else -> throw SQLException("Cannot compare $this and $other")
    }




    override fun equals(other: Any?): Boolean {
        return when {
            (this === other) -> true
            ((other is NumericCellValue)) -> (this.myValue.compareTo(other.myValue) == 0)
            ((other is IntegerCellValue)) -> (this.myValue.compareTo(BigDecimal.valueOf(other.myValue)) == 0)
            else -> false
        }
    }

    override fun hashCode(): Int {
        return myValue.toInt().hashCode()
    }

    override fun toString(): String {
        return "NumericCellValue(myValue=${myValue.toDouble()})"
    }

    override fun add(cellValue: CellValue): CellValue = when {
        cellValue == NullCellValue -> this
        cellValue is IntegerCellValue -> NumericCellValue(myValue.add(cellValue.myValue.toBigDecimal()))
        cellValue is NumericCellValue -> NumericCellValue(this.myValue.add(cellValue.myValue))
        else -> throw SQLException("Cannot add $cellValue to int")
    }
}