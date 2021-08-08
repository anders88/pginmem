package no.anksoft.pginmem.values

class ArrayCellValue(val myValues:List<CellValue>):CellValue {
    override fun valueAsText(): StringCellValue {
        TODO("Not yet implemented")
    }

    override fun valueAsInteger(): IntegerCellValue {
        TODO("Not yet implemented")
    }

    override fun valueAsBoolean(): BooleanCellValue {
        TODO("Not yet implemented")
    }

    override fun valueAsDate(): DateCellValue {
        TODO("Not yet implemented")
    }

    override fun valueAsTimestamp(): DateTimeCellValue {
        TODO("Not yet implemented")
    }

    override fun valueAsNumeric(): NumericCellValue {
        TODO("Not yet implemented")
    }

    override fun compareMeTo(other: CellValue): Int {
        TODO("Not yet implemented")
    }

    override fun add(cellValue: CellValue): CellValue {
        TODO("Not yet implemented")
    }
}