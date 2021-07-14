package no.anksoft.pginmem.clauses

import no.anksoft.pginmem.Cell
import no.anksoft.pginmem.statements.SelectStatement
import no.anksoft.pginmem.values.CellValue

class ExsistsClause(val selectStatement: SelectStatement,val doesExsists:Boolean):WhereClause {
    override fun isMatch(cells: List<Cell>): Boolean {
        val actuallyExsists = (selectStatement.internalExecuteQuery().numberOfRows > 0)
        return if (doesExsists) actuallyExsists else !actuallyExsists
    }

    override fun registerBinding(index: Int, value: CellValue): Boolean {
        return selectStatement.registerBinding(index,value)
    }
}