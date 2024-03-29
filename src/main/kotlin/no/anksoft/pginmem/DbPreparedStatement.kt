package no.anksoft.pginmem

import no.anksoft.pginmem.statements.*
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Date
import java.util.*


private val logger = LoggerFactory.getLogger(DbPreparedStatement::class.java)


private val relnames:List<List<Pair<String,Any?>>> = listOf(
listOf(Pair("relname","pg_shadow")),
listOf(Pair("relname","pg_settings")),
listOf(Pair("relname","pg_hba_file_rules")),
listOf(Pair("relname","pg_file_settings")),
listOf(Pair("relname","pg_config")),
listOf(Pair("relname","pg_replication_origin_status")),
listOf(Pair("relname","pg_stat_user_tables")),
listOf(Pair("relname","pg_stat_xact_user_tables")),
listOf(Pair("relname","pg_statio_all_tables")),
listOf(Pair("relname","pg_statio_sys_tables")),
listOf(Pair("relname","pg_statio_user_tables")),
listOf(Pair("relname","pg_stat_all_indexes")),
listOf(Pair("relname","pg_roles")),
listOf(Pair("relname","pg_locks")),
listOf(Pair("relname","pg_group")),
listOf(Pair("relname","pg_user")),
listOf(Pair("relname","pg_policies")),
listOf(Pair("relname","pg_rules")),
listOf(Pair("relname","pg_views")),
listOf(Pair("relname","pg_tables")),
listOf(Pair("relname","pg_matviews")),
listOf(Pair("relname","pg_indexes")),
listOf(Pair("relname","pg_sequences")),
listOf(Pair("relname","pg_stats")),
listOf(Pair("relname","pg_publication_tables")),
listOf(Pair("relname","pg_cursors")),
listOf(Pair("relname","pg_available_extensions")),
listOf(Pair("relname","pg_available_extension_versions")),
listOf(Pair("relname","pg_prepared_xacts")),
listOf(Pair("relname","pg_prepared_statements")),
listOf(Pair("relname","pg_seclabels")),
listOf(Pair("relname","pg_timezone_names")),
listOf(Pair("relname","pg_timezone_abbrevs")),
listOf(Pair("relname","pg_stat_all_tables")),
listOf(Pair("relname","pg_stat_xact_all_tables")),
listOf(Pair("relname","pg_stat_sys_tables")),
listOf(Pair("relname","pg_stat_xact_sys_tables")),
listOf(Pair("relname","pg_stat_sys_indexes")),
listOf(Pair("relname","pg_stat_user_indexes")),
listOf(Pair("relname","pg_statio_all_indexes")),
listOf(Pair("relname","pg_statio_sys_indexes")),
listOf(Pair("relname","pg_statio_user_indexes")),
listOf(Pair("relname","pg_statio_all_sequences")),
listOf(Pair("relname","pg_statio_sys_sequences")),
listOf(Pair("relname","pg_statio_user_sequences")),
listOf(Pair("relname","pg_stat_activity")),
listOf(Pair("relname","pg_stat_replication")),
listOf(Pair("relname","pg_stat_wal_receiver")),
listOf(Pair("relname","pg_stat_subscription")),
listOf(Pair("relname","pg_stat_ssl")),
listOf(Pair("relname","pg_replication_slots")),
listOf(Pair("relname","pg_stat_database")),
listOf(Pair("relname","pg_stat_database_conflicts")),
listOf(Pair("relname","pg_stat_user_functions")),
listOf(Pair("relname","pg_stat_xact_user_functions")),
listOf(Pair("relname","pg_stat_archiver")),
listOf(Pair("relname","pg_stat_bgwriter")),
listOf(Pair("relname","pg_stat_progress_vacuum")),
listOf(Pair("relname","pg_user_mappings")),
listOf(Pair("relname","information_schema_catalog_name")),
listOf(Pair("relname","attributes")),
listOf(Pair("relname","applicable_roles")),
listOf(Pair("relname","administrable_role_authorizations")),
listOf(Pair("relname","check_constraint_routine_usage")),
listOf(Pair("relname","character_sets")),
listOf(Pair("relname","check_constraints")),
listOf(Pair("relname","collations")),
listOf(Pair("relname","collation_character_set_applicability")),
listOf(Pair("relname","column_domain_usage")),
listOf(Pair("relname","column_privileges")),
listOf(Pair("relname","routine_privileges")),
listOf(Pair("relname","column_udt_usage")),
listOf(Pair("relname","columns")),
listOf(Pair("relname","constraint_column_usage")),
listOf(Pair("relname","role_routine_grants")),
listOf(Pair("relname","constraint_table_usage")),
listOf(Pair("relname","domain_constraints")),
listOf(Pair("relname","domain_udt_usage")),
listOf(Pair("relname","routines")),
listOf(Pair("relname","domains")),
listOf(Pair("relname","enabled_roles")),
listOf(Pair("relname","key_column_usage")),
listOf(Pair("relname","schemata")),
listOf(Pair("relname","parameters")),
listOf(Pair("relname","referential_constraints")),
listOf(Pair("relname","role_column_grants")),
listOf(Pair("relname","sequences")),
listOf(Pair("relname","view_routine_usage")),
listOf(Pair("relname","table_constraints")),
listOf(Pair("relname","table_privileges")),
listOf(Pair("relname","foreign_table_options")),
listOf(Pair("relname","role_table_grants")),
listOf(Pair("relname","view_table_usage")),
listOf(Pair("relname","tables")),
listOf(Pair("relname","transforms")),
listOf(Pair("relname","triggered_update_columns")),
listOf(Pair("relname","foreign_data_wrappers")),
listOf(Pair("relname","triggers")),
listOf(Pair("relname","views")),
listOf(Pair("relname","udt_privileges")),
listOf(Pair("relname","role_udt_grants")),
listOf(Pair("relname","usage_privileges")),
listOf(Pair("relname","data_type_privileges")),
listOf(Pair("relname","role_usage_grants")),
listOf(Pair("relname","user_defined_types")),
listOf(Pair("relname","_pg_foreign_servers")),
listOf(Pair("relname","view_column_usage")),
listOf(Pair("relname","element_types")),
listOf(Pair("relname","_pg_foreign_table_columns")),
listOf(Pair("relname","column_options")),
listOf(Pair("relname","_pg_foreign_data_wrappers")),
listOf(Pair("relname","foreign_server_options")),
listOf(Pair("relname","foreign_data_wrapper_options")),
listOf(Pair("relname","user_mapping_options")),
listOf(Pair("relname","foreign_servers")),
listOf(Pair("relname","_pg_foreign_tables")),
listOf(Pair("relname","foreign_tables")),
listOf(Pair("relname","_pg_user_mappings")),
listOf(Pair("relname","user_mappings")),
    )

fun createPreparedStatement(sql:String,dbTransaction: DbTransaction):DbPreparedStatement {
    logger.debug("Called $sql")
    val statementAnalyzer = StatementAnalyzer(sql)
    when {
        sql.toLowerCase().startsWith("select set_config('search_path',") -> return NoopStatement()
        sql.toLowerCase().startsWith("select pg_try_advisory_lock(") -> return StatementToReturnFixed(listOf(listOf(Pair("pg_try_advisory_lock","t"))),sql)
        sql.toLowerCase().startsWith("select pg_advisory_unlock(") -> return StatementToReturnFixed(listOf(listOf(Pair("pg_advisory_unlock","t"))),sql)
        sql.toLowerCase().startsWith("drop materialized view") -> return NoopStatement()
        sql.toLowerCase().startsWith("drop view if exists") -> return NoopStatement()
        sql.toLowerCase().startsWith("select proname, oidvectortypes(proargtypes)") -> return StatementToReturnFixed(emptyList(),sql)
        sql.toLowerCase().startsWith("select typname, typcategory from pg_catalog.pg_type") -> return StatementToReturnFixed(emptyList(),sql)
        sql.toLowerCase().startsWith("select t.table_name from information_schema.tables") -> return StatementToReturnFixed(emptyList(),sql)
        sql.toLowerCase().startsWith("select relname from pg_catalog.pg_class") -> return StatementToReturnFixed(relnames,sql)
        sql.toLowerCase().startsWith("select exists (") -> return StatementToReturnFixed(listOf(listOf(Pair("",true))),sql)
        sql.toLowerCase().startsWith("select count(*) from pg_namespace") -> return StatementToReturnFixed(listOf(listOf(Pair("",1))),sql)
        sql.toLowerCase().startsWith("select set_config") -> return NoopStatement()
        sql.toLowerCase().startsWith("set role") -> return NoopStatement()
        sql.toLowerCase() == "select current_schema" -> return SelectOneValueStatement("public")
        sql.toLowerCase() == "select current_user" -> return SelectOneValueStatement("localdevuser")
        sql.toLowerCase() == "select version()" -> return SelectOneValueStatement("PostgreSQL 10.6 Pginmemver")
        sql.toLowerCase() == "show search_path" -> return SelectOneValueStatement("\"\$user\", public")


        statementAnalyzer.word(0) == "create" && statementAnalyzer.word(1) == "table" -> return CreateTableStatement(statementAnalyzer,dbTransaction)
        statementAnalyzer.word(0) == "alter" && statementAnalyzer.word(1) == "table" -> return AlterTableStatement(statementAnalyzer, dbTransaction)
        statementAnalyzer.word(0) == "create" && statementAnalyzer.word(1) == "sequence" -> return CreateSequenceStatement(statementAnalyzer,dbTransaction)
        statementAnalyzer.word(0) == "alter" && statementAnalyzer.word(1) == "sequence" -> return AlterSequenceStatement(statementAnalyzer,dbTransaction)
        statementAnalyzer.word(0) == "create" && statementAnalyzer.word(1) == "index" -> return CreateIndexStatement(dbTransaction)
        statementAnalyzer.word(0) == "insert" && statementAnalyzer.word(1) == "into" -> return InsertIntoStatement(statementAnalyzer,dbTransaction,sql)
        statementAnalyzer.word(0) == "select" -> return SelectStatement(statementAnalyzer, dbTransaction)
        statementAnalyzer.word(0) == "update" -> return UpdateStatement(statementAnalyzer,dbTransaction)
        statementAnalyzer.word(0) == "delete" -> return DeleteStatement(statementAnalyzer,dbTransaction,sql)
        statementAnalyzer.word(0) == "drop" && statementAnalyzer.word(1) == "table" -> return DropTableStatement(statementAnalyzer,dbTransaction)
        else -> throw SQLException("Unknown statement ***$sql***")
    }
}

abstract class DbPreparedStatement(private val dbTransaction: DbTransaction?):PreparedStatement {
    private val logger = LoggerFactory.getLogger(this::class.java)
    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        TODO("Not yet implemented")
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean {
        TODO("Not yet implemented")
    }

    override fun close() {
    }

    override fun executeQuery(): ResultSet {
        TODO("Not yet implemented")
    }

    override fun executeQuery(sql: String?): ResultSet {
        TODO("Not yet implemented")
    }

    override fun executeUpdate(): Int {
        TODO("Not yet implemented")
    }

    override fun executeUpdate(sql: String?): Int {
        TODO("Not yet implemented")
    }

    override fun executeUpdate(sql: String?, autoGeneratedKeys: Int): Int {
        TODO("Not yet implemented")
    }

    override fun executeUpdate(sql: String?, columnIndexes: IntArray?): Int {
        TODO("Not yet implemented")
    }

    override fun executeUpdate(sql: String?, columnNames: Array<out String>?): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxFieldSize(): Int {
        TODO("Not yet implemented")
    }

    override fun setMaxFieldSize(max: Int) {
        TODO("Not yet implemented")
    }

    override fun getMaxRows(): Int {
        TODO("Not yet implemented")
    }

    override fun setMaxRows(max: Int) {
        logger.warn("Base setMaxRows $max")
    }

    override fun setEscapeProcessing(enable: Boolean) {
        logger.warn("Base setEscapeProcessing $enable")
    }

    override fun getQueryTimeout(): Int {
        TODO("Not yet implemented")
    }

    override fun setQueryTimeout(seconds: Int) {
        logger.warn("Base setQueryTimeout $seconds")

    }

    override fun cancel() {
        logger.warn("Base cancel")
    }

    override fun getWarnings(): SQLWarning {
        TODO("Not yet implemented")
    }

    override fun clearWarnings() {
        logger.warn("Base clearWarnings")
    }

    override fun setCursorName(name: String?) {
        logger.warn("Base setCursorName")
    }

    override fun execute(): Boolean {
        TODO("Not yet implemented")
    }

    override fun execute(sql: String?): Boolean {
        TODO("Not yet implemented")
    }

    override fun execute(sql: String?, autoGeneratedKeys: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun execute(sql: String?, columnIndexes: IntArray?): Boolean {
        TODO("Not yet implemented")
    }

    override fun execute(sql: String?, columnNames: Array<out String>?): Boolean {
        TODO("Not yet implemented")
    }

    override fun getResultSet(): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getUpdateCount(): Int {
        TODO("Not yet implemented")
    }

    override fun getMoreResults(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getMoreResults(current: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun setFetchDirection(direction: Int) {
        logger.warn("Base setFetchDirection $direction")
    }

    override fun getFetchDirection(): Int {
        TODO("Not yet implemented")
    }

    override fun setFetchSize(rows: Int) {
        logger.warn("Base setFetchSize $rows")
    }

    override fun getFetchSize(): Int {
        TODO("Not yet implemented")
    }

    override fun getResultSetConcurrency(): Int {
        TODO("Not yet implemented")
    }

    override fun getResultSetType(): Int {
        TODO("Not yet implemented")
    }

    override fun addBatch() {
        logger.warn("Base addBatch")

    }

    override fun addBatch(sql: String?) {
        logger.warn("Base $sql")
    }

    override fun clearBatch() {
        logger.warn("Base clearBatch")
    }

    override fun executeBatch(): IntArray {
        TODO("Not yet implemented")
    }

    override fun getConnection(): Connection {
        return DbConnection(dbTransaction?:throw SQLException("No connection found"))
    }

    override fun getGeneratedKeys(): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getResultSetHoldability(): Int {
        TODO("Not yet implemented")
    }

    override fun isClosed(): Boolean {
        TODO("Not yet implemented")
    }

    override fun setPoolable(poolable: Boolean) {
        logger.warn("Base setPoolable $poolable")
    }

    override fun isPoolable(): Boolean {
        TODO("Not yet implemented")
    }

    override fun closeOnCompletion() {
        logger.warn("Base closeOnCompletion")
    }

    override fun isCloseOnCompletion(): Boolean {
        TODO("Not yet implemented")
    }

    override fun setNull(parameterIndex: Int, sqlType: Int) {
        logger.warn("Base setNull $parameterIndex $sqlType")
    }

    override fun setNull(parameterIndex: Int, sqlType: Int, typeName: String?) {
        logger.warn("Base setNull")
    }

    override fun setBoolean(parameterIndex: Int, x: Boolean) {
        logger.warn("Base setBoolean")
    }

    override fun setByte(parameterIndex: Int, x: Byte) {
        logger.warn("Base setByte")
    }

    override fun setShort(parameterIndex: Int, x: Short) {
        logger.warn("Base setShort")
    }

    override fun setInt(parameterIndex: Int, x: Int) {
        logger.warn("Base setInt")
    }

    override fun setLong(parameterIndex: Int, x: Long) {
        logger.warn("Base setLong")
    }

    override fun setFloat(parameterIndex: Int, x: Float) {
        logger.warn("Base setFloat")
    }

    override fun setDouble(parameterIndex: Int, x: Double) {
        logger.warn("Base setDouble")
    }

    override fun setBigDecimal(parameterIndex: Int, x: BigDecimal?) {
        logger.warn("Base setBigDecimal")
    }

    override fun setString(parameterIndex: Int, x: String?) {
        logger.warn("Base setString $parameterIndex $x")
    }

    override fun setBytes(parameterIndex: Int, x: ByteArray?) {
        logger.warn("Base setBytes")
    }

    override fun setDate(parameterIndex: Int, x: Date?) {
        logger.warn("Base setDate only date")
    }

    override fun setDate(parameterIndex: Int, x: Date?, cal: Calendar?) {
        logger.warn("Base setDate with calendar")
    }

    override fun setTime(parameterIndex: Int, x: Time?) {
        logger.warn("Base setTime")
    }

    override fun setTime(parameterIndex: Int, x: Time?, cal: Calendar?) {
        logger.warn("Base setTime")
    }

    override fun setTimestamp(parameterIndex: Int, x: Timestamp?) {
        logger.warn("Base setTimestamp")
    }

    override fun setTimestamp(parameterIndex: Int, x: Timestamp?, cal: Calendar?) {
        logger.warn("Base setTimestamp")
    }

    override fun setAsciiStream(parameterIndex: Int, x: InputStream?, length: Int) {
        logger.warn("Base setAsciiStream")
    }

    override fun setAsciiStream(parameterIndex: Int, x: InputStream?, length: Long) {
        logger.warn("Base setAsciiStream")
    }

    override fun setAsciiStream(parameterIndex: Int, x: InputStream?) {
        logger.warn("Base setAsciiStream")
    }

    override fun setUnicodeStream(parameterIndex: Int, x: InputStream?, length: Int) {
        logger.warn("Base setUnicodeStream")
    }

    override fun setBinaryStream(parameterIndex: Int, x: InputStream?, length: Int) {
        logger.warn("Base setBinaryStream")
    }

    override fun setBinaryStream(parameterIndex: Int, x: InputStream?, length: Long) {
        logger.warn("Base setBinaryStream")
    }

    override fun setBinaryStream(parameterIndex: Int, x: InputStream?) {
        logger.warn("Base setBinaryStream")
    }

    override fun clearParameters() {
        logger.warn("Base clearParameters")
    }

    override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: Int) {
        logger.warn("Base setObject")
    }

    override fun setObject(parameterIndex: Int, x: Any?) {
        logger.warn("Base setObject")
    }

    override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: Int, scaleOrLength: Int) {
        logger.warn("Base setObject")
    }

    override fun setCharacterStream(parameterIndex: Int, reader: Reader?, length: Int) {
        logger.warn("Base setCharacterStream")
    }

    override fun setCharacterStream(parameterIndex: Int, reader: Reader?, length: Long) {
        logger.warn("Base setCharacterStream")
    }

    override fun setCharacterStream(parameterIndex: Int, reader: Reader?) {
        logger.warn("Base setCharacterStream")
    }

    override fun setRef(parameterIndex: Int, x: Ref?) {
        logger.warn("Base setRef")
    }

    override fun setBlob(parameterIndex: Int, x: Blob?) {
        logger.warn("Base setBlob")
    }

    override fun setBlob(parameterIndex: Int, inputStream: InputStream?, length: Long) {
        logger.warn("Base setBlob")
    }

    override fun setBlob(parameterIndex: Int, inputStream: InputStream?) {
        logger.warn("Base setBlob")
    }

    override fun setClob(parameterIndex: Int, x: Clob?) {
        logger.warn("Base setClob")
    }

    override fun setClob(parameterIndex: Int, reader: Reader?, length: Long) {
        logger.warn("Base setClob")
    }

    override fun setClob(parameterIndex: Int, reader: Reader?) {
        logger.warn("Base setClob")
    }

    override fun setArray(parameterIndex: Int, x: java.sql.Array?) {
        logger.warn("Base setArray")
    }

    override fun getMetaData(): ResultSetMetaData {
        TODO("Not yet implemented")
    }

    override fun setURL(parameterIndex: Int, x: URL?) {
        logger.warn("Base setURL")
    }

    override fun getParameterMetaData(): ParameterMetaData {
        TODO("Not yet implemented")
    }

    override fun setRowId(parameterIndex: Int, x: RowId?) {
        logger.warn("Base setRowId")
    }

    override fun setNString(parameterIndex: Int, value: String?) {
        logger.warn("Base setNString")
    }

    override fun setNCharacterStream(parameterIndex: Int, value: Reader?, length: Long) {
        logger.warn("Base setNCharacterStream")
    }

    override fun setNCharacterStream(parameterIndex: Int, value: Reader?) {
        logger.warn("Base setNCharacterStream")
    }

    override fun setNClob(parameterIndex: Int, value: NClob?) {
        logger.warn("Base setNClob")
    }

    override fun setNClob(parameterIndex: Int, reader: Reader?, length: Long) {
        logger.warn("Base setNClob")
    }

    override fun setNClob(parameterIndex: Int, reader: Reader?) {
        logger.warn("Base setNClob")
    }

    override fun setSQLXML(parameterIndex: Int, xmlObject: SQLXML?) {
        logger.warn("Base setSQLXML")
    }


}