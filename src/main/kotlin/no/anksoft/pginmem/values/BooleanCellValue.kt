package no.anksoft.pginmem.values

class BooleanCellValue(private val myValue:Boolean):CellValue {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as BooleanCellValue

        if (myValue != other.myValue) return false

        return true
    }

    override fun hashCode(): Int {
        return myValue.hashCode()
    }
}