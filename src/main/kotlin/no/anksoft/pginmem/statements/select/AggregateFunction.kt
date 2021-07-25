package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.values.CellValue
import no.anksoft.pginmem.values.IntegerCellValue
import no.anksoft.pginmem.values.NullCellValue
import no.anksoft.pginmem.values.NumericCellValue

interface AggregateFunction {
    fun newInstanse():AggregateFunction
    fun emptyResultValue():CellValue
    fun addValue(addedValue:CellValue):CellValue
}

abstract class SimpleAggregateFunction:AggregateFunction {
    abstract fun aggregate(a:CellValue,b:CellValue):CellValue

    private var totalValue:CellValue? = null

    override fun addValue(addedValue: CellValue):CellValue {
        val current:CellValue? = totalValue
        if (current == null) {
            totalValue = addedValue
            return addedValue
        }
        val newtotal:CellValue = aggregate(current,addedValue)
        totalValue = newtotal
        return newtotal
    }
}

class MaxAggregateFunction:SimpleAggregateFunction() {
    override fun aggregate(a: CellValue, b: CellValue): CellValue {
        if (a == NullCellValue) return b
        if (b == NullCellValue) return a
        return if (a.compareMeTo(b,false) > 0) a else b
    }

    override fun newInstanse(): AggregateFunction = MaxAggregateFunction()

    override fun emptyResultValue(): CellValue = NullCellValue
}

class MinAggregateFunction:SimpleAggregateFunction() {
    override fun aggregate(a: CellValue, b: CellValue): CellValue {
        if (a == NullCellValue) return b
        if (b == NullCellValue) return a
        return if (a.compareMeTo(b,false) < 0) a else b
    }

    override fun newInstanse(): AggregateFunction = MinAggregateFunction()

    override fun emptyResultValue(): CellValue = NullCellValue
}

open class SumAggregateFunction:SimpleAggregateFunction() {
    override fun aggregate(a: CellValue, b: CellValue): CellValue {
        return a.add(b)
    }

    override fun newInstanse(): AggregateFunction = SumAggregateFunction()

    override fun emptyResultValue(): CellValue = NullCellValue
}

class CountAggregateFunction:SumAggregateFunction() {
    override fun emptyResultValue(): CellValue = IntegerCellValue(0)
    override fun newInstanse(): AggregateFunction = CountAggregateFunction()
}

class CountDistinctAggregateFunction:AggregateFunction {
    private val valuesAdded:MutableSet<CellValue> = mutableSetOf()

    override fun newInstanse(): AggregateFunction = CountDistinctAggregateFunction()

    override fun emptyResultValue(): CellValue = IntegerCellValue(0L)

    override fun addValue(addedValue: CellValue): CellValue {
        valuesAdded.add(addedValue)
        return IntegerCellValue(valuesAdded.size.toLong())
    }
}


