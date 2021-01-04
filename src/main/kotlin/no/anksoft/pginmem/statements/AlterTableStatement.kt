package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import java.sql.SQLException

class AlterTableStatement(private val statementAnalyzer: StatementAnalyzer, private val  dbTransaction: DbTransaction) : DbPreparedStatement() {

    override fun executeUpdate(): Int {
        statementAnalyzer.addIndex(2)
        var table = dbTransaction.tableForUpdate(statementAnalyzer.word()?:throw SQLException("Unexpected end of statemnt"))

        do {
            statementAnalyzer.addIndex()
            val command = statementAnalyzer.word()
            statementAnalyzer.addIndex()
            if (statementAnalyzer.word() == "column") {
                statementAnalyzer.addIndex()
            }

            val newTable: Table? = when {
                command == "alter" -> alterColumn(table)
                command == "add" -> addColumn(table)
                command == "drop" -> deleteColumn(table)
                command == "rename" && statementAnalyzer.word() == "to" -> renameTable(
                    table,
                    statementAnalyzer.addIndex().word()
                )
                command == "rename" -> renameColumn(table)
                else -> throw SQLException("Unknown alter table command $command")
            }
            if (newTable != null) {
                dbTransaction.registerTableUpdate(newTable)
                table = newTable
            }
        } while (statementAnalyzer.addIndex().word() == ",")
        return 0
    }

    private fun alterColumn(table: Table): Table {
        val colname = statementAnalyzer.word()?:throw SQLException("Expected column name")
        val column:Column = table.findColumn(colname)?:throw SQLException("Unknown column $colname")
        statementAnalyzer.addIndex()
        if (statementAnalyzer.word() == "drop" && statementAnalyzer.word(1) == "default") {
            statementAnalyzer.addIndex()
            val newCol = column.setDefault(null)
            return replaceCol(table,column,newCol,null)
        }
        if (statementAnalyzer.word() == "type") {
            statementAnalyzer.addIndex()
            val coltypetext = statementAnalyzer.word()?:throw SQLException("Expected columnt type")
            val newColumnType:ColumnType = ColumnType.values().firstOrNull { it.matchesColumnType(coltypetext) }?:throw SQLException("Unkown columnt type $coltypetext")

            val (convertToType,sourceCol) = if ( statementAnalyzer.word(1) == "using") {
                val sourceColText =
                    statementAnalyzer.addIndex(2).word() ?: throw SQLException("Expected column name after using")
                val sourceCol = table.findColumn(sourceColText) ?: throw SQLException("Unknown column $colname")

                val convertToType: ColumnType? = if (statementAnalyzer.word(1) == "::") {
                    val colToTypeText =
                        statementAnalyzer.addIndex(2).word() ?: throw SQLException("Expected type after ::")
                    ColumnType.values().firstOrNull { it.matchesColumnType(colToTypeText) }
                } else null
                Pair(convertToType,sourceCol)
            } else Pair(null,null)
            val newColumn = column.changeColumnType(newColumnType)
            val valueTransformation:((Row)->Any?)? = if (convertToType != null) { row ->
                val currentValue = row.cells.firstOrNull { it.column == sourceCol }?.value
                column.columnType.convertValue(convertToType,currentValue)
            } else null

            return replaceCol(table,column,newColumn,valueTransformation)
        }
        if (statementAnalyzer.word() == "set" && statementAnalyzer.word(1) == "default") {
            statementAnalyzer.addIndex(2)
            val readValueFromExpression = statementAnalyzer.readValueFromExpression(dbTransaction, emptyMap())
            val newCol = column.setDefault(readValueFromExpression.valuegen)
            return replaceCol(table,column,newCol,null)

        }
        throw SQLException("Unknown alter column command ${statementAnalyzer.word()}")
    }

    private fun replaceCol(table: Table,oldcol:Column, newColumn: Column,valueTransformation:((Row)->Any?)?):Table {
        val newCols = table.colums.map {
            if (it == oldcol) newColumn else it
        }
        val newTable = Table(table.name,newCols)
        for (row in table.rowsForReading()) {
            val newCells = row.cells.map {
                if (it.column == oldcol)
                    Cell(newColumn,if (valueTransformation !=null) valueTransformation.invoke(row) else it.value)
                else it
            }
            newTable.addRow(Row(newCells))
        }
        return newTable
    }

    private fun renameTable(table: Table,toName:String?):Table? {
        if (toName == null) {
            throw SQLException("Missing name to rename to")
        }
        val newCols = table.colums.map { it.renameTable(toName) }
        val newTable = Table(toName,newCols)
        table.rowsForReading().forEach { exrow ->
            val newRow = Row(exrow.cells.map { Cell(it.column.renameTable(toName),it.value) })
            newTable.addRow(newRow)
        }
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
        if (table.colums.any { it.matches(table.name,newColName) }) {
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
            val newCell = Cell(newColumn, newColumn.defaultValue?.invoke(Pair(dbTransaction,row)))
            adjustedCells.add(newCell)
            newTable.addRow(Row(adjustedCells))
        }
        return newTable
    }

}
