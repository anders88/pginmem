package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import java.sql.SQLException

class AlterTableStatement(private val statementAnalyzer: StatementAnalyzer, private val  dbTransaction: DbTransaction) : DbPreparedStatement() {

    override fun executeUpdate(): Int {
        statementAnalyzer.addIndex(2)
        val table = dbTransaction.tableForUpdate(statementAnalyzer.word()?:throw SQLException("Unexpected end of statemnt"))

        statementAnalyzer.addIndex()
        if (statementAnalyzer.word() != "add") {
            throw SQLException("Expected add")
        }
        statementAnalyzer.addIndex()
        if (statementAnalyzer.word() == "column") {
            statementAnalyzer.addIndex()
        }
        val newColumn = Column.create(statementAnalyzer)
        val adjustedColumns = table.colums.toMutableList()
        adjustedColumns.add(newColumn)
        val newTable = Table(table.name,adjustedColumns)
        for (row in table.rowsForReading()) {
            val adjustedCells = row.cells.toMutableList()
            val newCell = Cell(newColumn,newColumn.defaultValue?.invoke())
            adjustedCells.add(newCell)
            newTable.addRow(Row(adjustedCells))
        }
        dbTransaction.registerTableUpdate(newTable)
        return 0
    }

}
