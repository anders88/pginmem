package no.anksoft.pginmem.values

import java.time.LocalDateTime

class DateTimeCellValue(private val myValue:LocalDateTime):CellValue {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as DateTimeCellValue

        if (myValue != other.myValue) return false

        return true
    }

    override fun hashCode(): Int {
        return myValue.hashCode()
    }
}