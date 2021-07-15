package no.anksoft.pginmem.values

import no.anksoft.pginmem.ColumnType
import java.sql.Array
import java.sql.ResultSet
import java.sql.SQLException

private fun columntTypeFromTypename(typeName: String?):ColumnType {
    if (typeName == null) {
        throw SQLException("Typename  cannot be null in sqlarray")
    }
    val res = ColumnType.values().firstOrNull { it.matchesColumnType(typeName) }
    return res?:throw SQLException("Unknown type $typeName")
}

class SqlArray(typeName: String?, elements: kotlin.Array<out Any>?):Array {

    val arrayCellValue:ArrayCellValue
    init {
        if (elements == null) {
            throw SQLException("Array cannot be null")
        }
        val columnType:ColumnType = columntTypeFromTypename(typeName)
        val list = elements.map { columnType.readFromAny(it) }.toList()
        arrayCellValue = ArrayCellValue(list)
    }
    override fun getBaseTypeName(): String {
        TODO("Not yet implemented")
    }

    override fun getBaseType(): Int {
        TODO("Not yet implemented")
    }

    override fun getArray(): Any {
        TODO("Not yet implemented")
    }

    override fun getArray(map: MutableMap<String, Class<*>>?): Any {
        TODO("Not yet implemented")
    }

    override fun getArray(index: Long, count: Int): Any {
        TODO("Not yet implemented")
    }

    override fun getArray(index: Long, count: Int, map: MutableMap<String, Class<*>>?): Any {
        TODO("Not yet implemented")
    }

    override fun getResultSet(): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getResultSet(map: MutableMap<String, Class<*>>?): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getResultSet(index: Long, count: Int): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getResultSet(index: Long, count: Int, map: MutableMap<String, Class<*>>?): ResultSet {
        TODO("Not yet implemented")
    }

    override fun free() {
        TODO("Not yet implemented")
    }
}