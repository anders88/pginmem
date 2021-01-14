package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.values.CellValue
import no.anksoft.pginmem.values.NullCellValue

interface AggregateFunction {
    fun aggregate(a:CellValue,b:CellValue):CellValue
}

class MaxAggregateFunction:AggregateFunction {
    override fun aggregate(a: CellValue, b: CellValue): CellValue {
        if (a == NullCellValue) return b
        if (b == NullCellValue) return a
        return if (a.compareMeTo(b,false) > 0) a else b
    }
}

class MinAggregateFunction:AggregateFunction {
    override fun aggregate(a: CellValue, b: CellValue): CellValue {
        if (a == NullCellValue) return b
        if (b == NullCellValue) return a
        return if (a.compareMeTo(b,false) < 0) a else b
    }
}

class SumAggregateFunction:AggregateFunction {
    override fun aggregate(a: CellValue, b: CellValue): CellValue {
        return a.add(b)
    }

}
