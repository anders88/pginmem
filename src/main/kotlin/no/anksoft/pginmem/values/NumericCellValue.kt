package no.anksoft.pginmem.values

import java.math.BigDecimal

class NumericCellValue(private val myValue: BigDecimal):CellValue {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as NumericCellValue

        if (myValue != other.myValue) return false

        return true
    }

    override fun hashCode(): Int {
        return myValue.hashCode()
    }
}