package no.anksoft.pginmem.values

import org.jsonbuddy.JsonObject
import java.sql.SQLException

class JsonCellValue(val myvalue:JsonObject):CellValue {
    override fun valueAsText(): StringCellValue = StringCellValue(myvalue.toJson())

    override fun valueAsInteger(): IntegerCellValue {
        throw SQLException("Cannot cast jsonvalue to integer")
    }

    override fun valueAsBoolean(): BooleanCellValue {
        throw SQLException("Cannot cast jsonvalue to boolean")
    }

    override fun valueAsDate(): DateCellValue {
        throw SQLException("Cannot cast jsonvalue to date")
    }

    override fun valueAsTimestamp(): DateTimeCellValue {
        throw SQLException("Cannot cast jsonvalue to datetime")

    }

    override fun valueAsNumeric(): NumericCellValue {
        throw SQLException("Cannot cast jsonvalue to numeric")
    }

    override fun compareMeTo(other: CellValue, nullsFirst: Boolean): Int {
        throw SQLException("Cannot comapare jsonvalue")
    }

    override fun add(cellValue: CellValue): CellValue {
        throw SQLException("Cannot add jsonvalue")
    }
}