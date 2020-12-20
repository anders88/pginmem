package no.anksoft.pginmem

class Sequence(val name:String) {
    private var currentValue = 0L

    fun nextVal():Long {
        currentValue++
        return currentValue
    }
}