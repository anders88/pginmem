package no.anksoft.pginmem

import no.anksoft.pginmem.values.CellValue
import no.anksoft.pginmem.values.DateTimeCellValue
import no.anksoft.pginmem.values.StringCellValue
import java.sql.SQLException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

private val datetimewithfractionformat = "yyyy-MM-dd HH:mm:ss.SSSSSS"

private fun adjustCellValue(columnType: ColumnType,input:CellValue):CellValue {
    if (columnType == ColumnType.TIMESTAMP && input is StringCellValue) {
        if (input.myValue.length != datetimewithfractionformat.length) {
            throw SQLException("Unknown thimestamp '${input.myValue}'")
        }
        val formatter = DateTimeFormatter.ofPattern(datetimewithfractionformat)
        val localDateTime = LocalDateTime.parse(input.myValue,formatter)
        return DateTimeCellValue(localDateTime)
    }
    return input
}

class Cell(val column: Column,inputCellValue: CellValue) {
    val value:CellValue = adjustCellValue(column.columnType,inputCellValue)
    override fun toString(): String {
        return "Cell(column=$column, value=$value)"
    }


}