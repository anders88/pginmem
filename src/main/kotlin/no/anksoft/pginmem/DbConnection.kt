package no.anksoft.pginmem

import no.anksoft.pginmem.statements.DbStatement
import java.sql.*
import java.util.*
import java.util.concurrent.Executor

class DbConnection(private val transaction: DbTransaction):Connection {

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        TODO("Not yet implemented")
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean {
        TODO("Not yet implemented")
    }

    override fun close() {
        transaction.close()
    }

    override fun createStatement(): Statement {
        return DbStatement(transaction)
    }

    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement {
        TODO("Not yet implemented")
    }

    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement {
        TODO("Not yet implemented")
    }

    override fun prepareStatement(sql: String?): PreparedStatement {
        return createPreparedStatement(sql!!,transaction)
    }

    override fun prepareStatement(sql: String?, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement {
        TODO("Not yet implemented")
    }

    override fun prepareStatement(
        sql: String?,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): PreparedStatement {
        TODO("Not yet implemented")
    }

    override fun prepareStatement(sql: String?, autoGeneratedKeys: Int): PreparedStatement {
        TODO("Not yet implemented")
    }

    override fun prepareStatement(sql: String?, columnIndexes: IntArray?): PreparedStatement {
        TODO("Not yet implemented")
    }

    override fun prepareStatement(sql: String?, columnNames: Array<out String>?): PreparedStatement {
        TODO("Not yet implemented")
    }

    override fun prepareCall(sql: String?): CallableStatement {
        TODO("Not yet implemented")
    }

    override fun prepareCall(sql: String?, resultSetType: Int, resultSetConcurrency: Int): CallableStatement {
        TODO("Not yet implemented")
    }

    override fun prepareCall(
        sql: String?,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): CallableStatement {
        TODO("Not yet implemented")
    }

    override fun nativeSQL(sql: String?): String {
        TODO("Not yet implemented")
    }

    override fun setAutoCommit(autoCommit: Boolean) {
        transaction.setAutoCommit(autoCommit)
    }

    override fun getAutoCommit(): Boolean {
        return transaction.isAutoCommit()
    }

    override fun commit() {
        transaction.commit()
    }

    override fun rollback() {
        transaction.rollback()
    }

    override fun rollback(savepoint: Savepoint?) {
        TODO("Not yet implemented")
    }

    override fun isClosed(): Boolean {
        return transaction.isClosed()
    }

    override fun getMetaData(): DatabaseMetaData {
        return DbDatabaseMetadata()
    }

    override fun setReadOnly(readOnly: Boolean) {

    }

    override fun isReadOnly(): Boolean {
        return false
    }

    override fun setCatalog(catalog: String?) {
        TODO("Not yet implemented")
    }

    override fun getCatalog(): String {
        return "public"
    }

    override fun setTransactionIsolation(level: Int) {
        TODO("Not yet implemented")
    }

    override fun getTransactionIsolation(): Int {
        return Connection.TRANSACTION_READ_UNCOMMITTED
    }

    override fun getWarnings(): SQLWarning {
        TODO("Not yet implemented")
    }

    override fun clearWarnings() {
        TODO("Not yet implemented")
    }

    override fun getTypeMap(): MutableMap<String, Class<*>> {
        TODO("Not yet implemented")
    }

    override fun setTypeMap(map: MutableMap<String, Class<*>>?) {
        TODO("Not yet implemented")
    }

    override fun setHoldability(holdability: Int) {
        TODO("Not yet implemented")
    }

    override fun getHoldability(): Int {
        TODO("Not yet implemented")
    }

    override fun setSavepoint(): Savepoint {
        TODO("Not yet implemented")
    }

    override fun setSavepoint(name: String?): Savepoint {
        TODO("Not yet implemented")
    }

    override fun releaseSavepoint(savepoint: Savepoint?) {
        TODO("Not yet implemented")
    }

    override fun createClob(): Clob {
        TODO("Not yet implemented")
    }

    override fun createBlob(): Blob {
        TODO("Not yet implemented")
    }

    override fun createNClob(): NClob {
        TODO("Not yet implemented")
    }

    override fun createSQLXML(): SQLXML {
        TODO("Not yet implemented")
    }

    override fun isValid(timeout: Int): Boolean {
        return true
    }

    override fun setClientInfo(name: String?, value: String?) {
        TODO("Not yet implemented")
    }

    override fun setClientInfo(properties: Properties?) {
        TODO("Not yet implemented")
    }

    override fun getClientInfo(name: String?): String {
        TODO("Not yet implemented")
    }

    override fun getClientInfo(): Properties {
        TODO("Not yet implemented")
    }

    override fun createArrayOf(typeName: String?, elements: Array<out Any>?): java.sql.Array {
        TODO("Not yet implemented")
    }

    override fun createStruct(typeName: String?, attributes: Array<out Any>?): Struct {
        TODO("Not yet implemented")
    }

    override fun setSchema(schema: String?) {
        TODO("Not yet implemented")
    }

    override fun getSchema(): String {
        TODO("Not yet implemented")
    }

    override fun abort(executor: Executor?) {
        TODO("Not yet implemented")
    }

    override fun setNetworkTimeout(executor: Executor?, milliseconds: Int) {

    }

    override fun getNetworkTimeout(): Int {
        return 0
    }
}