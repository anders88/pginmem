package no.anksoft.pginmem.values

import java.sql.SQLException

class ByteArrayCellValue(givenArr:ByteArray):CellValue {
    private val myValue:ByteArray = givenArr.clone()
    override fun valueAsText(): StringCellValue {
        throw SQLException("Cannot read bytearray as text")
    }

    override fun valueAsInteger(): IntegerCellValue {
        throw SQLException("Cannot read bytearray as integer")
    }

    override fun valueAsBoolean(): BooleanCellValue {
        throw SQLException("Cannot read bytearray as boolean")

    }

    override fun valueAsDate(): DateCellValue {
        throw SQLException("Cannot read bytearray as date")

    }

    override fun valueAsTimestamp(): DateTimeCellValue {
        throw SQLException("Cannot read bytearray as timestamp")
    }

    override fun valueAsNumeric(): NumericCellValue {
        throw SQLException("Cannot read bytearray as numeric")
    }

    val myBytes:ByteArray get() = myValue.clone()
}