package no.anksoft.pginmem

import no.anksoft.pginmem.values.IntegerCellValue

class Sequence(val name:String) {
    private var currentValue = 0L

    fun nextVal():IntegerCellValue {
        currentValue++
        return IntegerCellValue(currentValue)
    }
}