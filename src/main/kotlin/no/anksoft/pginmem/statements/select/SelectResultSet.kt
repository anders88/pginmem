package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.DbTransaction
import no.anksoft.pginmem.Row
import no.anksoft.pginmem.values.ByteArrayCellValue
import no.anksoft.pginmem.values.CellValue
import no.anksoft.pginmem.values.NullCellValue
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Array
import java.sql.Date
import java.util.*
import kotlin.collections.ArrayList


private fun computeSelectSet(
    colums: List<SelectColumnProvider>,
    selectRowProvider: SelectRowProvider,
    dbTransaction: DbTransaction,
    disinctFlag: Boolean
):List<List<CellValue>> {
    val res:MutableList<List<CellValue>> = mutableListOf()
    for (i in 0 until selectRowProvider.size()) {
        val row:Row = selectRowProvider.readRow(i)
        val valuesThisRow:List<CellValue> = colums.map { colProvider ->
            colProvider.valueFromExpression.valuegen.invoke(Pair(dbTransaction,row))
        }
        if (disinctFlag) {
            var isDistinct=true
            for (exsisting in res) {
                var disinctThisRow = false
                for (j in exsisting.indices) {
                    if (exsisting[j] != valuesThisRow[j]) {
                        disinctThisRow = true
                        break
                    }
                }
                isDistinct = disinctThisRow
                if (!isDistinct) {
                    break
                }
            }
            if (!isDistinct) {
                continue
            }
        }
        res.add(valuesThisRow)
    }
    if (colums.none { it.aggregateFunction != null }) {
        return res
    }
    val resArr:ArrayList<List<CellValue>> = ArrayList()

    for (genrow in res) {
        var matchRow:Int? = null
        for (index in 0 until resArr.size) {
            val alreadyFound = resArr[index]
            var isMatch = true
            for (i in 0 until colums.size) {
                if (colums[i].aggregateFunction != null) {
                    continue
                }
                if (genrow[i] != alreadyFound[i]) {
                    isMatch = false
                    break
                }
            }
            if (isMatch) {
                matchRow = index
                break
            }
        }
        if (matchRow == null) {
            resArr.add(genrow)
            continue
        }
        val currentMatch = resArr[matchRow]
        val combinedRow:List<CellValue> = colums.mapIndexed { myindex,acol ->
            if (acol.aggregateFunction == null) {
                currentMatch[myindex]
            } else {
                acol.aggregateFunction.aggregate(currentMatch[myindex],genrow[myindex])
            }
        }
        resArr.set(matchRow,combinedRow)
    }

    return resArr
}


class SelectResultSet(
    val colums: List<SelectColumnProvider>,
    val selectRowProviderGiven: SelectRowProvider,
    dbTransaction: DbTransaction,
    disinctFlag:Boolean,
):ResultSet {


    private val selectSet:List<List<CellValue>> by lazy {
        computeSelectSet(colums,selectRowProviderGiven,dbTransaction,disinctFlag)
    }



    val numberOfRows:Int by lazy { selectSet.size}



    fun valueAt(columnIndex:Int,rowIndex:Int):CellValue {
        val ind = colums.indexOfFirst { it.colindex == columnIndex }
        if (ind == -1) {
            throw SQLException("Illegal column no $columnIndex")
        }
        return selectSet[rowIndex][ind]
    }

    fun valueAt(columnLabel:String,rowno: Int):CellValue {
        val columnProvider:SelectColumnProvider = colums.firstOrNull { it.isMatch(columnLabel.toLowerCase()) }
            ?:throw SQLException("Unknown column $columnLabel")
        val value = valueAt(columnProvider.colindex,rowno)
        return value
    }

    private var rowindex =  selectRowProviderGiven.offset-1
    private var lastWasNull:Boolean = false

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        TODO("Not yet implemented")
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean {
        TODO("Not yet implemented")
    }

    override fun close() {

    }

    override fun next(): Boolean {
        rowindex++
        val limit:Int? = selectRowProviderGiven.limit
        return (rowindex < numberOfRows && (limit == null || rowindex < limit))
    }

    override fun getString(columnLabel: String?): String? {
        val value = readCell(columnLabel)
        return if (value == NullCellValue) null else value.valueAsText().myValue
    }

    override fun getString(columnIndex: Int): String? {
        val value = readCell(columnIndex)
        return if (value == NullCellValue) null else value.valueAsText().myValue
    }


    fun readCell(columnLabel: String?): CellValue {
        if (columnLabel == null) {
            throw SQLException("Cannot get null")
        }

        val columnProvider:SelectColumnProvider = colums.firstOrNull { it.isMatch(columnLabel.toLowerCase()) }
            ?:throw SQLException("Unknown column $columnLabel")
        val value = valueAt(columnProvider.colindex,rowindex)
        lastWasNull = (value == NullCellValue)
        return value
    }

    private fun readCell(columnIndex: Int):CellValue {
        val value = valueAt(columnIndex,rowindex)
        lastWasNull = (value == NullCellValue)
        return value
    }

    override fun getTimestamp(columnLabel: String?): Timestamp? {
        val value = readCell(columnLabel)
        if (value == NullCellValue) return null
        return Timestamp.valueOf(value.valueAsTimestamp().myValue)
    }

    private fun getInt(value:CellValue):Int {
        if (value == NullCellValue) return 0
        return value.valueAsInteger().myValue.toInt()
    }

    override fun getInt(columnLabel: String?): Int {
        return getInt(readCell(columnLabel))

    }

    override fun getInt(columnIndex: Int): Int {
        return getInt(readCell(columnIndex))
    }



    override fun wasNull(): Boolean = lastWasNull



    override fun getBoolean(columnIndex: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun getBoolean(columnLabel: String?): Boolean {
        val value = readCell(columnLabel)
        return value.valueAsBoolean().myValue
    }

    override fun getByte(columnIndex: Int): Byte {
        TODO("Not yet implemented")
    }

    override fun getByte(columnLabel: String?): Byte {
        TODO("Not yet implemented")
    }

    override fun getShort(columnIndex: Int): Short {
        TODO("Not yet implemented")
    }

    override fun getShort(columnLabel: String?): Short {
        TODO("Not yet implemented")
    }


    private fun getLong(value:CellValue):Long {
        if (value == NullCellValue) return 0L
        return value.valueAsInteger().myValue
    }

    private fun getBigDecimal(value: CellValue):BigDecimal? {
        if (value == NullCellValue) return null
        return value.valueAsNumeric().myValue
    }

    override fun getBigDecimal(columnIndex: Int): BigDecimal? {
        return getBigDecimal(readCell(columnIndex))
    }

    override fun getBigDecimal(columnLabel: String?): BigDecimal? {
        return getBigDecimal(readCell(columnLabel))
    }

    private fun getByteArray(value:CellValue):ByteArray? {
        if (value == NullCellValue) return null
        if (value !is ByteArrayCellValue) throw SQLException("Not bytearray  value")
        return value.myBytes
    }

    override fun getBytes(columnIndex: Int): ByteArray? {
        return getByteArray(readCell(columnIndex))
    }

    override fun getBytes(columnLabel: String?): ByteArray? {
        return getByteArray(readCell(columnLabel))

    }


    override fun getLong(columnIndex: Int): Long {
        return getLong(readCell(columnIndex))
    }

    override fun getLong(columnLabel: String?): Long {
        return getLong(readCell(columnLabel))
    }

    override fun getFloat(columnIndex: Int): Float {
        TODO("Not yet implemented")
    }

    override fun getFloat(columnLabel: String?): Float {
        TODO("Not yet implemented")
    }

    override fun getDouble(columnIndex: Int): Double {
        TODO("Not yet implemented")
    }

    override fun getDouble(columnLabel: String?): Double {
        TODO("Not yet implemented")
    }

    override fun getBigDecimal(columnIndex: Int, scale: Int): BigDecimal {
        TODO("Not yet implemented")
    }

    override fun getBigDecimal(columnLabel: String?, scale: Int): BigDecimal {
        TODO("Not yet implemented")
    }



    override fun getDate(columnIndex: Int): Date {
        TODO("Not yet implemented")
    }

    override fun getDate(columnLabel: String?): Date {
        TODO("Not yet implemented")
    }

    override fun getDate(columnIndex: Int, cal: Calendar?): Date {
        TODO("Not yet implemented")
    }

    override fun getDate(columnLabel: String?, cal: Calendar?): Date {
        TODO("Not yet implemented")
    }

    override fun getTime(columnIndex: Int): Time {
        TODO("Not yet implemented")
    }

    override fun getTime(columnLabel: String?): Time {
        TODO("Not yet implemented")
    }

    override fun getTime(columnIndex: Int, cal: Calendar?): Time {
        TODO("Not yet implemented")
    }

    override fun getTime(columnLabel: String?, cal: Calendar?): Time {
        TODO("Not yet implemented")
    }

    override fun getTimestamp(columnIndex: Int): Timestamp {
        TODO("Not yet implemented")
    }


    override fun getTimestamp(columnIndex: Int, cal: Calendar?): Timestamp {
        TODO("Not yet implemented")
    }

    override fun getTimestamp(columnLabel: String?, cal: Calendar?): Timestamp {
        TODO("Not yet implemented")
    }

    override fun getAsciiStream(columnIndex: Int): InputStream {
        TODO("Not yet implemented")
    }

    override fun getAsciiStream(columnLabel: String?): InputStream {
        TODO("Not yet implemented")
    }

    override fun getUnicodeStream(columnIndex: Int): InputStream {
        TODO("Not yet implemented")
    }

    override fun getUnicodeStream(columnLabel: String?): InputStream {
        TODO("Not yet implemented")
    }

    override fun getBinaryStream(columnIndex: Int): InputStream {
        TODO("Not yet implemented")
    }

    override fun getBinaryStream(columnLabel: String?): InputStream {
        TODO("Not yet implemented")
    }

    override fun getWarnings(): SQLWarning {
        TODO("Not yet implemented")
    }

    override fun clearWarnings() {
        TODO("Not yet implemented")
    }

    override fun getCursorName(): String {
        TODO("Not yet implemented")
    }

    override fun getMetaData(): ResultSetMetaData {
        TODO("Not yet implemented")
    }

    override fun getObject(columnIndex: Int): Any {
        TODO("Not yet implemented")
    }

    override fun getObject(columnLabel: String?): Any {
        TODO("Not yet implemented")
    }

    override fun getObject(columnIndex: Int, map: MutableMap<String, Class<*>>?): Any {
        TODO("Not yet implemented")
    }

    override fun getObject(columnLabel: String?, map: MutableMap<String, Class<*>>?): Any {
        TODO("Not yet implemented")
    }

    override fun <T : Any?> getObject(columnIndex: Int, type: Class<T>?): T {
        TODO("Not yet implemented")
    }

    override fun <T : Any?> getObject(columnLabel: String?, type: Class<T>?): T {
        TODO("Not yet implemented")
    }

    override fun findColumn(columnLabel: String?): Int {
        TODO("Not yet implemented")
    }

    override fun getCharacterStream(columnIndex: Int): Reader {
        TODO("Not yet implemented")
    }

    override fun getCharacterStream(columnLabel: String?): Reader {
        TODO("Not yet implemented")
    }

    override fun isBeforeFirst(): Boolean {
        TODO("Not yet implemented")
    }

    override fun isAfterLast(): Boolean {
        TODO("Not yet implemented")
    }

    override fun isFirst(): Boolean {
        TODO("Not yet implemented")
    }

    override fun isLast(): Boolean {
        TODO("Not yet implemented")
    }

    override fun beforeFirst() {
        TODO("Not yet implemented")
    }

    override fun afterLast() {
        TODO("Not yet implemented")
    }

    override fun first(): Boolean {
        TODO("Not yet implemented")
    }

    override fun last(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getRow(): Int {
        TODO("Not yet implemented")
    }

    override fun absolute(row: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun relative(rows: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun previous(): Boolean {
        TODO("Not yet implemented")
    }

    override fun setFetchDirection(direction: Int) {
        TODO("Not yet implemented")
    }

    override fun getFetchDirection(): Int {
        TODO("Not yet implemented")
    }

    override fun setFetchSize(rows: Int) {
        TODO("Not yet implemented")
    }

    override fun getFetchSize(): Int {
        TODO("Not yet implemented")
    }

    override fun getType(): Int {
        TODO("Not yet implemented")
    }

    override fun getConcurrency(): Int {
        TODO("Not yet implemented")
    }

    override fun rowUpdated(): Boolean {
        TODO("Not yet implemented")
    }

    override fun rowInserted(): Boolean {
        TODO("Not yet implemented")
    }

    override fun rowDeleted(): Boolean {
        TODO("Not yet implemented")
    }

    override fun updateNull(columnIndex: Int) {
        TODO("Not yet implemented")
    }

    override fun updateNull(columnLabel: String?) {
        TODO("Not yet implemented")
    }

    override fun updateBoolean(columnIndex: Int, x: Boolean) {
        TODO("Not yet implemented")
    }

    override fun updateBoolean(columnLabel: String?, x: Boolean) {
        TODO("Not yet implemented")
    }

    override fun updateByte(columnIndex: Int, x: Byte) {
        TODO("Not yet implemented")
    }

    override fun updateByte(columnLabel: String?, x: Byte) {
        TODO("Not yet implemented")
    }

    override fun updateShort(columnIndex: Int, x: Short) {
        TODO("Not yet implemented")
    }

    override fun updateShort(columnLabel: String?, x: Short) {
        TODO("Not yet implemented")
    }

    override fun updateInt(columnIndex: Int, x: Int) {
        TODO("Not yet implemented")
    }

    override fun updateInt(columnLabel: String?, x: Int) {
        TODO("Not yet implemented")
    }

    override fun updateLong(columnIndex: Int, x: Long) {
        TODO("Not yet implemented")
    }

    override fun updateLong(columnLabel: String?, x: Long) {
        TODO("Not yet implemented")
    }

    override fun updateFloat(columnIndex: Int, x: Float) {
        TODO("Not yet implemented")
    }

    override fun updateFloat(columnLabel: String?, x: Float) {
        TODO("Not yet implemented")
    }

    override fun updateDouble(columnIndex: Int, x: Double) {
        TODO("Not yet implemented")
    }

    override fun updateDouble(columnLabel: String?, x: Double) {
        TODO("Not yet implemented")
    }

    override fun updateBigDecimal(columnIndex: Int, x: BigDecimal?) {
        TODO("Not yet implemented")
    }

    override fun updateBigDecimal(columnLabel: String?, x: BigDecimal?) {
        TODO("Not yet implemented")
    }

    override fun updateString(columnIndex: Int, x: String?) {
        TODO("Not yet implemented")
    }

    override fun updateString(columnLabel: String?, x: String?) {
        TODO("Not yet implemented")
    }

    override fun updateBytes(columnIndex: Int, x: ByteArray?) {
        TODO("Not yet implemented")
    }

    override fun updateBytes(columnLabel: String?, x: ByteArray?) {
        TODO("Not yet implemented")
    }

    override fun updateDate(columnIndex: Int, x: Date?) {
        TODO("Not yet implemented")
    }

    override fun updateDate(columnLabel: String?, x: Date?) {
        TODO("Not yet implemented")
    }

    override fun updateTime(columnIndex: Int, x: Time?) {
        TODO("Not yet implemented")
    }

    override fun updateTime(columnLabel: String?, x: Time?) {
        TODO("Not yet implemented")
    }

    override fun updateTimestamp(columnIndex: Int, x: Timestamp?) {
        TODO("Not yet implemented")
    }

    override fun updateTimestamp(columnLabel: String?, x: Timestamp?) {
        TODO("Not yet implemented")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Int) {
        TODO("Not yet implemented")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Int) {
        TODO("Not yet implemented")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?) {
        TODO("Not yet implemented")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?) {
        TODO("Not yet implemented")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Int) {
        TODO("Not yet implemented")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Int) {
        TODO("Not yet implemented")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?) {
        TODO("Not yet implemented")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?) {
        TODO("Not yet implemented")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Int) {
        TODO("Not yet implemented")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Int) {
        TODO("Not yet implemented")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?) {
        TODO("Not yet implemented")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?) {
        TODO("Not yet implemented")
    }

    override fun updateObject(columnIndex: Int, x: Any?, scaleOrLength: Int) {
        TODO("Not yet implemented")
    }

    override fun updateObject(columnIndex: Int, x: Any?) {
        TODO("Not yet implemented")
    }

    override fun updateObject(columnLabel: String?, x: Any?, scaleOrLength: Int) {
        TODO("Not yet implemented")
    }

    override fun updateObject(columnLabel: String?, x: Any?) {
        TODO("Not yet implemented")
    }

    override fun insertRow() {
        TODO("Not yet implemented")
    }

    override fun updateRow() {
        TODO("Not yet implemented")
    }

    override fun deleteRow() {
        TODO("Not yet implemented")
    }

    override fun refreshRow() {
        TODO("Not yet implemented")
    }

    override fun cancelRowUpdates() {
        TODO("Not yet implemented")
    }

    override fun moveToInsertRow() {
        TODO("Not yet implemented")
    }

    override fun moveToCurrentRow() {
        TODO("Not yet implemented")
    }

    override fun getStatement(): Statement {
        TODO("Not yet implemented")
    }

    override fun getRef(columnIndex: Int): Ref {
        TODO("Not yet implemented")
    }

    override fun getRef(columnLabel: String?): Ref {
        TODO("Not yet implemented")
    }

    override fun getBlob(columnIndex: Int): Blob {
        TODO("Not yet implemented")
    }

    override fun getBlob(columnLabel: String?): Blob {
        TODO("Not yet implemented")
    }

    override fun getClob(columnIndex: Int): Clob {
        TODO("Not yet implemented")
    }

    override fun getClob(columnLabel: String?): Clob {
        TODO("Not yet implemented")
    }

    override fun getArray(columnIndex: Int): Array {
        TODO("Not yet implemented")
    }

    override fun getArray(columnLabel: String?): Array {
        TODO("Not yet implemented")
    }

    override fun getURL(columnIndex: Int): URL {
        TODO("Not yet implemented")
    }

    override fun getURL(columnLabel: String?): URL {
        TODO("Not yet implemented")
    }

    override fun updateRef(columnIndex: Int, x: Ref?) {
        TODO("Not yet implemented")
    }

    override fun updateRef(columnLabel: String?, x: Ref?) {
        TODO("Not yet implemented")
    }

    override fun updateBlob(columnIndex: Int, x: Blob?) {
        TODO("Not yet implemented")
    }

    override fun updateBlob(columnLabel: String?, x: Blob?) {
        TODO("Not yet implemented")
    }

    override fun updateBlob(columnIndex: Int, inputStream: InputStream?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateBlob(columnLabel: String?, inputStream: InputStream?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateBlob(columnIndex: Int, inputStream: InputStream?) {
        TODO("Not yet implemented")
    }

    override fun updateBlob(columnLabel: String?, inputStream: InputStream?) {
        TODO("Not yet implemented")
    }

    override fun updateClob(columnIndex: Int, x: Clob?) {
        TODO("Not yet implemented")
    }

    override fun updateClob(columnLabel: String?, x: Clob?) {
        TODO("Not yet implemented")
    }

    override fun updateClob(columnIndex: Int, reader: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateClob(columnLabel: String?, reader: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateClob(columnIndex: Int, reader: Reader?) {
        TODO("Not yet implemented")
    }

    override fun updateClob(columnLabel: String?, reader: Reader?) {
        TODO("Not yet implemented")
    }

    override fun updateArray(columnIndex: Int, x: Array?) {
        TODO("Not yet implemented")
    }

    override fun updateArray(columnLabel: String?, x: Array?) {
        TODO("Not yet implemented")
    }

    override fun getRowId(columnIndex: Int): RowId {
        TODO("Not yet implemented")
    }

    override fun getRowId(columnLabel: String?): RowId {
        TODO("Not yet implemented")
    }

    override fun updateRowId(columnIndex: Int, x: RowId?) {
        TODO("Not yet implemented")
    }

    override fun updateRowId(columnLabel: String?, x: RowId?) {
        TODO("Not yet implemented")
    }

    override fun getHoldability(): Int {
        TODO("Not yet implemented")
    }

    override fun isClosed(): Boolean {
        TODO("Not yet implemented")
    }

    override fun updateNString(columnIndex: Int, nString: String?) {
        TODO("Not yet implemented")
    }

    override fun updateNString(columnLabel: String?, nString: String?) {
        TODO("Not yet implemented")
    }

    override fun updateNClob(columnIndex: Int, nClob: NClob?) {
        TODO("Not yet implemented")
    }

    override fun updateNClob(columnLabel: String?, nClob: NClob?) {
        TODO("Not yet implemented")
    }

    override fun updateNClob(columnIndex: Int, reader: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateNClob(columnLabel: String?, reader: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateNClob(columnIndex: Int, reader: Reader?) {
        TODO("Not yet implemented")
    }

    override fun updateNClob(columnLabel: String?, reader: Reader?) {
        TODO("Not yet implemented")
    }

    override fun getNClob(columnIndex: Int): NClob {
        TODO("Not yet implemented")
    }

    override fun getNClob(columnLabel: String?): NClob {
        TODO("Not yet implemented")
    }

    override fun getSQLXML(columnIndex: Int): SQLXML {
        TODO("Not yet implemented")
    }

    override fun getSQLXML(columnLabel: String?): SQLXML {
        TODO("Not yet implemented")
    }

    override fun updateSQLXML(columnIndex: Int, xmlObject: SQLXML?) {
        TODO("Not yet implemented")
    }

    override fun updateSQLXML(columnLabel: String?, xmlObject: SQLXML?) {
        TODO("Not yet implemented")
    }

    override fun getNString(columnIndex: Int): String {
        TODO("Not yet implemented")
    }

    override fun getNString(columnLabel: String?): String {
        TODO("Not yet implemented")
    }

    override fun getNCharacterStream(columnIndex: Int): Reader {
        TODO("Not yet implemented")
    }

    override fun getNCharacterStream(columnLabel: String?): Reader {
        TODO("Not yet implemented")
    }

    override fun updateNCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
        TODO("Not yet implemented")
    }

    override fun updateNCharacterStream(columnIndex: Int, x: Reader?) {
        TODO("Not yet implemented")
    }

    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?) {
        TODO("Not yet implemented")
    }
}