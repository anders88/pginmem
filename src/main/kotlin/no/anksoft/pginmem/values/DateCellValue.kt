package no.anksoft.pginmem.values

import java.time.LocalDate

class DateCellValue(private val myValue:LocalDate):CellValue {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as DateCellValue

        if (myValue != other.myValue) return false

        return true
    }

    override fun hashCode(): Int {
        return myValue.hashCode()
    }
}