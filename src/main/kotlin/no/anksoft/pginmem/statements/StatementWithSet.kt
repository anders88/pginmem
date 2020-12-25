package no.anksoft.pginmem.statements

import no.anksoft.pginmem.DbPreparedStatement
import java.math.BigDecimal
import java.sql.Timestamp

abstract class StatementWithSet:DbPreparedStatement() {
    abstract fun setSomething(parameterIndex: Int, x: Any?)

    override fun setString(parameterIndex: Int, x: String?) {
        setSomething(parameterIndex,x)
    }

    override fun setTimestamp(parameterIndex: Int, x: Timestamp?) {
        setSomething(parameterIndex,x)
    }

    override fun setInt(parameterIndex: Int, x: Int) {
        setSomething(parameterIndex,x)
    }


    override fun setBoolean(parameterIndex: Int, x: Boolean) {
        setSomething(parameterIndex,x)
    }

    override fun setDouble(parameterIndex: Int, x: Double) {
        setSomething(parameterIndex, x)
    }

    override fun setBigDecimal(parameterIndex: Int, x: BigDecimal?) {
        setSomething(parameterIndex, x)
    }

    override fun setBytes(parameterIndex: Int, x: ByteArray?) {
        setSomething(parameterIndex,x)
    }


}