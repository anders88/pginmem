package no.anksoft.pginmem.statements

import no.anksoft.pginmem.DbPreparedStatement
import no.anksoft.pginmem.values.*
import java.math.BigDecimal
import java.sql.Timestamp

abstract class StatementWithSet:DbPreparedStatement() {
    abstract fun setSomething(parameterIndex: Int, x: CellValue)

    override fun setString(parameterIndex: Int, x: String?) {
        setSomething(parameterIndex,x?.let { StringCellValue(it) }?:NullCellValue)
    }

    override fun setTimestamp(parameterIndex: Int, x: Timestamp?) {
        setSomething(parameterIndex,x?.let { DateTimeCellValue(it.toLocalDateTime()) }?:NullCellValue)
    }

    override fun setInt(parameterIndex: Int, x: Int) {
        setSomething(parameterIndex,IntegerCellValue(x.toLong()))
    }

    override fun setLong(parameterIndex: Int, x: Long) {
        setSomething(parameterIndex,IntegerCellValue(x))
    }


    override fun setBoolean(parameterIndex: Int, x: Boolean) {
        setSomething(parameterIndex,BooleanCellValue(x))
    }

    override fun setDouble(parameterIndex: Int, x: Double) {
        setSomething(parameterIndex, NumericCellValue(BigDecimal.valueOf(x)))
    }

    override fun setBigDecimal(parameterIndex: Int, x: BigDecimal?) {
        setSomething(parameterIndex, x?.let { NumericCellValue(it) }?:NullCellValue)
    }

    override fun setBytes(parameterIndex: Int, x: ByteArray?) {
        setSomething(parameterIndex,x?.let { ByteArrayCellValue(it) }?:NullCellValue)
    }


}