package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import java.sql.SQLException

class AlterTableStatement(private val statementAnalyzer: StatementAnalyzer, private val  dbTransaction: DbTransaction) : DbPreparedStatement() {

    override fun executeUpdate(): Int {
        statementAnalyzer.addIndex(2)
        val table = dbTransaction.tableForUpdate(statementAnalyzer.word()?:throw SQLException("Unexpected end of statemnt"))

        statementAnalyzer.addIndex()
        val command = statementAnalyzer.word()
        statementAnalyzer.addIndex()
        if (statementAnalyzer.word() == "column") {
            statementAnalyzer.addIndex()
        }

        val newTable:Table? = when  {
            command == "add" -> addColumn(table)
            command == "drop" -> deleteColumn(table)
            command == "rename" && statementAnalyzer.word() == "to" -> renameTable(table,statementAnalyzer.addIndex().word())
            command == "rename" -> renameColumn(table)
            else -> throw SQLException("Unknown alter table command $command")
        }
        newTable?.let {  dbTransaction.registerTableUpdate(it)}
        return 0
    }

    private fun renameTable(table: Table,toName:String?):Table? {
        if (toName == null) {
            throw SQLException("Missing name to rename to")
        }
        val newTable = Table(toName,table.colums)
        table.rowsForReading().forEach({newTable.addRow(it)})
        dbTransaction.removeTable(table)
        dbTransaction.createAlterTableSetup(newTable)
        return null

    }

    private fun renameColumn(table: Table): Table {
        val colnameFrom = statementAnalyzer.word()?:throw SQLException("Expected colunname from")
        val columnToRename:Column = table.findColumn(colnameFrom)?:throw SQLException("Unkown column $colnameFrom")
        statementAnalyzer.addIndex()
        if (statementAnalyzer.word() != "to") {
            throw SQLException("Expected to")
        }
        statementAnalyzer.addIndex()
        val newColName = stripSeachName(statementAnalyzer.word()?:throw SQLException("Unexpected end of sequence"))
        if (table.colums.any { it.name == newColName }) {
            throw SQLException("New columnname already exsists")
        }
        val newColumn = columnToRename.rename(newColName)
        val adjustedColumns = table.colums.map { oldcol ->
            if (oldcol == columnToRename) newColumn else oldcol
        }
        val newTable = Table(table.name,adjustedColumns)
        for (row in table.rowsForReading()) {
            val adjustedCells = row.cells.map {
                if (it.column == columnToRename) {
                    Cell(newColumn,it.value)
                } else it
            }
            newTable.addRow(Row(adjustedCells))
        }
        return newTable
    }

    private fun deleteColumn(table: Table): Table {
        val colname = statementAnalyzer.word()?:throw SQLException("Expected column name")
        val columnToDelete:Column = table.findColumn(colname)?:throw SQLException("Unknown column $colname")
        val adjustedColumns = table.colums.filter { it != columnToDelete }
        val newTable = Table(table.name, adjustedColumns)
        for (row in table.rowsForReading()) {
            val adjustedCells = row.cells.filter { it.column != columnToDelete }
            newTable.addRow(Row(adjustedCells))
        }
        return newTable
    }

    private fun addColumn(table: Table): Table {
        val newColumn = Column.create(table.name,statementAnalyzer,dbTransaction)
        val adjustedColumns = table.colums.toMutableList()
        adjustedColumns.add(newColumn)
        val newTable = Table(table.name, adjustedColumns)
        for (row in table.rowsForReading()) {
            val adjustedCells = row.cells.toMutableList()
            val newCell = Cell(newColumn, newColumn.defaultValue?.invoke(dbTransaction))
            adjustedCells.add(newCell)
            newTable.addRow(Row(adjustedCells))
        }
        return newTable
    }

}
