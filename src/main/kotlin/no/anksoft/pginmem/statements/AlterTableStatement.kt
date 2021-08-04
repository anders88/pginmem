package no.anksoft.pginmem.statements

import no.anksoft.pginmem.*
import no.anksoft.pginmem.values.NullCellValue
import java.sql.SQLException

class AlterTableStatement(private val statementAnalyzer: StatementAnalyzer, private val  dbTransaction: DbTransaction) : DbPreparedStatement(dbTransaction) {

    override fun executeUpdate(): Int {
        statementAnalyzer.addIndex(2)
        val cancelIfTableNotExsist = if (statementAnalyzer.word() == "if" && statementAnalyzer.word(1) == "exists") {
            statementAnalyzer.addIndex(2)
            true
        } else false
        val tableToUpdateName = statementAnalyzer.word()
        val tableToUpate: Table = dbTransaction.tableForUpdateIdOrNull(tableToUpdateName ?:throw SQLException("Unexpected end of statemnt"))
            ?: if (cancelIfTableNotExsist) {
                return 0
            } else {
                throw SQLException("Unknown table $tableToUpdateName")
            }

        var table:Table = tableToUpate
        do {
            statementAnalyzer.addIndex()
            val command = statementAnalyzer.word()
            statementAnalyzer.addIndex()

            if (statementAnalyzer.word() == "column") {
                statementAnalyzer.addIndex()
            }

            val onlyIfExists:Boolean = if (statementAnalyzer.word() == "if" && statementAnalyzer.word(1) == "exists") {
                statementAnalyzer.addIndex(2)
                true
            } else false
            val onlyIfNotExsists:Boolean = if (statementAnalyzer.word() == "if" && statementAnalyzer.word(1) == "not" && statementAnalyzer.word(2) == "exists") {
                statementAnalyzer.addIndex(3)
                true
            } else false

            if (statementAnalyzer.word() == "column") {
                statementAnalyzer.addIndex()
            }

            val newTable: Table? = when {
                command == "alter" -> alterColumn(table,onlyIfExists,onlyIfNotExsists)
                command == "add" -> addColumn(table,onlyIfExists,onlyIfNotExsists)
                command == "drop" -> deleteColumn(table,onlyIfExists,onlyIfNotExsists)
                command == "rename" && statementAnalyzer.word() == "to" -> renameTable(
                    table,
                    statementAnalyzer.addIndex().word()
                )
                command == "rename" -> renameColumn(table,onlyIfExists,onlyIfNotExsists)
                else -> throw SQLException("Unknown alter table command $command")
            }
            if (newTable != null) {
                dbTransaction.registerTableUpdate(newTable)
                table = newTable
            }
        } while (statementAnalyzer.addIndex().word() == ",")
        return 0
    }

    private fun alterColumn(table: Table, onlyIfExists: Boolean, onlyIfNotExsists: Boolean): Table {
        if (onlyIfNotExsists) {
            throw SQLException("Cannot use not exsists with alter column")
        }
        val colname = statementAnalyzer.word()?:throw SQLException("Expected column name")
        val column:Column = table.findColumn(colname)?:if (onlyIfExists) {
            statementAnalyzer.addIndexUntilNextCommaOrEnd()
            return table
        } else throw SQLException("Unknown column $colname")
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

            val sourceValue:ValueFromExpression? = if ( statementAnalyzer.word(1) == "using") {
                statementAnalyzer.addIndex(2)
                statementAnalyzer.readValueFromExpression(dbTransaction,mapOf(Pair(table.name,table)),null)
            } else null
            val newColumn = column.changeColumnType(newColumnType)

            return replaceCol(table,column,newColumn,sourceValue)
        }
        if (statementAnalyzer.word() == "set" && statementAnalyzer.word(1) == "default") {
            statementAnalyzer.addIndex(2)
            val readValueFromExpression = statementAnalyzer.readValueFromExpression(dbTransaction, emptyMap(),null)
            val newCol = column.setDefault(readValueFromExpression)
            return replaceCol(table,column,newCol,null)
        }
        if (statementAnalyzer.word() == "set" && (statementAnalyzer.word(1) == "null" || (statementAnalyzer.word(1) == "not" && statementAnalyzer.word(2) == "null"))) {
            return table
        }
        throw SQLException("Unknown alter column command ${statementAnalyzer.word()}")
    }

    private fun replaceCol(table: Table,oldcol:Column, newColumn: Column,valueTransformation:ValueFromExpression?):Table {
        val newCols = table.colums.map {
            if (it == oldcol) newColumn else it
        }
        val newTable = Table(table.name,newCols)
        for (row in table.rowsForReading()) {
            val newCells = row.cells.map {
                if (it.column == oldcol)
                    Cell(newColumn,if (valueTransformation !=null) valueTransformation.genereateValue(dbTransaction,row) else it.value)
                else it
            }
            newTable.addRow(Row(newCells,row.rowids))
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
            val newRow = Row(exrow.cells.map { Cell(it.column.renameTable(toName),it.value) },exrow.rowids)
            newTable.addRow(newRow)
        }
        dbTransaction.removeTable(table)
        dbTransaction.createAlterTableSetup(newTable)
        return null

    }

    private fun renameColumn(table: Table, onlyIfExists: Boolean, onlyIfNotExsists: Boolean): Table {
        if (onlyIfNotExsists) {
            throw SQLException("Cannot rename non exsisting column")
        }
        val colnameFrom = statementAnalyzer.word()?:throw SQLException("Expected colunname from")
        val columnToRename:Column = table.findColumn(colnameFrom)?:if (onlyIfExists) {
            statementAnalyzer.addIndexUntilNextCommaOrEnd()
            return table
        } else {
            throw SQLException("Unkown column $colnameFrom")
        }
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
            newTable.addRow(Row(adjustedCells,row.rowids))
        }
        return newTable
    }

    private fun deleteColumn(table: Table, onlyIfExists: Boolean, onlyIfNotExsists: Boolean): Table {
        if (onlyIfNotExsists) {
            throw SQLException("Cannot delete column that not exsist")
        }
        val colname = statementAnalyzer.word()?:throw SQLException("Expected column name")
        val columnToDelete:Column = table.findColumn(colname)?:if (onlyIfExists) {
            return table
        } else {
            throw SQLException("Unknown column $colname")
        }
        val adjustedColumns = table.colums.filter { it != columnToDelete }
        val newTable = Table(table.name, adjustedColumns)
        for (row in table.rowsForReading()) {
            val adjustedCells = row.cells.filter { it.column != columnToDelete }
            newTable.addRow(Row(adjustedCells,row.rowids))
        }
        return newTable
    }

    private fun addColumn(table: Table, onlyIfExists: Boolean, onlyIfNotExsists: Boolean): Table {
        if (onlyIfExists) {
            throw SQLException("Cannot add column with if exsists")
        }
        val columnName:String = statementAnalyzer.word()?:throw SQLException("Expecting column name")
        if (onlyIfNotExsists && table.findColumn(columnName) != null) {
            statementAnalyzer.addIndexUntilNextCommaOrEnd()
            return table
        }
        val newColumn = Column.create(table.name,statementAnalyzer,dbTransaction)
        val adjustedColumns = table.colums.toMutableList()
        adjustedColumns.add(newColumn)
        val newTable = Table(table.name, adjustedColumns)
        for (row in table.rowsForReading()) {
            val adjustedCells = row.cells.toMutableList()
            val newCell = Cell(newColumn, newColumn.defaultValue?.genereateValue(dbTransaction,row)?:NullCellValue)
            adjustedCells.add(newCell)
            newTable.addRow(Row(adjustedCells,row.rowids))
        }
        return newTable
    }

}
