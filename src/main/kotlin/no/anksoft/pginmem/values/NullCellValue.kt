package no.anksoft.pginmem.values

import java.sql.SQLException

object NullCellValue:CellValue {
    override fun valueAsText(): StringCellValue {
        TODO("Not yet implemented")
    }

    override fun valueAsInteger(): IntegerCellValue {
        TODO("Not yet implemented")
    }

    override fun valueAsBoolean(): BooleanCellValue = BooleanCellValue(false)

    override fun valueAsDate(): DateCellValue {
        TODO("Not yet implemented")
    }

    override fun valueAsTimestamp(): DateTimeCellValue {
        TODO("Not yet implemented")
    }

    override fun valueAsNumeric(): NumericCellValue {
        TODO("Not yet implemented")
    }

    override fun compareMeTo(other: CellValue): Int = when (other) {
        this -> 0
        else -> throw SQLException("Cannot compare NullCellvalue to $other")
    }

    override fun toString(): String {
        return "NullCellValue"
    }

    override fun add(cellValue: CellValue): CellValue = cellValue
}