package no.anksoft.pginmem.values

interface CellValue {
    fun valueAsText():StringCellValue
    fun valueAsInteger():IntegerCellValue
    fun valueAsBoolean():BooleanCellValue
    fun valueAsDate():DateCellValue
    fun valueAsTimestamp():DateTimeCellValue
    fun valueAsNumeric():NumericCellValue

    fun compareMeTo(other: CellValue):Int

    fun add(cellValue: CellValue):CellValue
}