package no.anksoft.pginmem.values

class ByteArrayCellValue(givenArr:ByteArray) {
    private val myValue:ByteArray = givenArr.clone()
}