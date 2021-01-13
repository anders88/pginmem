package no.anksoft.pginmem.values

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

    override fun compareMeTo(other: CellValue, nullsFirst: Boolean): Int = when (other) {
        this -> 0
        else -> if (nullsFirst) -1 else 1
    }

    override fun toString(): String {
        return "NullCellValue"
    }
}