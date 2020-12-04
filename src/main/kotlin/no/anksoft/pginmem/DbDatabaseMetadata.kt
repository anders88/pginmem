package no.anksoft.pginmem

import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet
import java.sql.RowIdLifetime

class DbDatabaseMetadata:DatabaseMetaData {
    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        TODO("Not yet implemented")
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean {
        TODO("Not yet implemented")
    }

    override fun allProceduresAreCallable(): Boolean {
        return false
    }

    override fun allTablesAreSelectable(): Boolean {
        return true
    }

    override fun getURL(): String? {
        return null
    }

    override fun getUserName(): String? {
        return null
    }

    override fun isReadOnly(): Boolean {
        return false
    }

    override fun nullsAreSortedHigh(): Boolean {
        return true
    }

    override fun nullsAreSortedLow(): Boolean {
        return false
    }

    override fun nullsAreSortedAtStart(): Boolean {
        return false
    }

    override fun nullsAreSortedAtEnd(): Boolean {
        return true
    }

    override fun getDatabaseProductName(): String {
        return "PostgreSQL"
    }

    override fun getDatabaseProductVersion(): String {
        return "0.0.1"
    }

    override fun getDriverName(): String {
        return "pginmemdriver"
    }

    override fun getDriverVersion(): String {
        return "0.0.1"
    }

    override fun getDriverMajorVersion(): Int {
        return 0
    }

    override fun getDriverMinorVersion(): Int {
        TODO("Not yet implemented")
    }

    override fun usesLocalFiles(): Boolean {
        TODO("Not yet implemented")
    }

    override fun usesLocalFilePerTable(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsMixedCaseIdentifiers(): Boolean {
        TODO("Not yet implemented")
    }

    override fun storesUpperCaseIdentifiers(): Boolean {
        TODO("Not yet implemented")
    }

    override fun storesLowerCaseIdentifiers(): Boolean {
        TODO("Not yet implemented")
    }

    override fun storesMixedCaseIdentifiers(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsMixedCaseQuotedIdentifiers(): Boolean {
        TODO("Not yet implemented")
    }

    override fun storesUpperCaseQuotedIdentifiers(): Boolean {
        TODO("Not yet implemented")
    }

    override fun storesLowerCaseQuotedIdentifiers(): Boolean {
        TODO("Not yet implemented")
    }

    override fun storesMixedCaseQuotedIdentifiers(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getIdentifierQuoteString(): String {
        TODO("Not yet implemented")
    }

    override fun getSQLKeywords(): String {
        TODO("Not yet implemented")
    }

    override fun getNumericFunctions(): String {
        TODO("Not yet implemented")
    }

    override fun getStringFunctions(): String {
        TODO("Not yet implemented")
    }

    override fun getSystemFunctions(): String {
        TODO("Not yet implemented")
    }

    override fun getTimeDateFunctions(): String {
        TODO("Not yet implemented")
    }

    override fun getSearchStringEscape(): String {
        TODO("Not yet implemented")
    }

    override fun getExtraNameCharacters(): String {
        TODO("Not yet implemented")
    }

    override fun supportsAlterTableWithAddColumn(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsAlterTableWithDropColumn(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsColumnAliasing(): Boolean {
        TODO("Not yet implemented")
    }

    override fun nullPlusNonNullIsNull(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsConvert(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsConvert(fromType: Int, toType: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsTableCorrelationNames(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsDifferentTableCorrelationNames(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsExpressionsInOrderBy(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsOrderByUnrelated(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsGroupBy(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsGroupByUnrelated(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsGroupByBeyondSelect(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsLikeEscapeClause(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsMultipleResultSets(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsMultipleTransactions(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsNonNullableColumns(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsMinimumSQLGrammar(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsCoreSQLGrammar(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsExtendedSQLGrammar(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsANSI92EntryLevelSQL(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsANSI92IntermediateSQL(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsANSI92FullSQL(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsIntegrityEnhancementFacility(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsOuterJoins(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsFullOuterJoins(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsLimitedOuterJoins(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getSchemaTerm(): String {
        TODO("Not yet implemented")
    }

    override fun getProcedureTerm(): String {
        TODO("Not yet implemented")
    }

    override fun getCatalogTerm(): String {
        TODO("Not yet implemented")
    }

    override fun isCatalogAtStart(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getCatalogSeparator(): String {
        TODO("Not yet implemented")
    }

    override fun supportsSchemasInDataManipulation(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsSchemasInProcedureCalls(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsSchemasInTableDefinitions(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsSchemasInIndexDefinitions(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsSchemasInPrivilegeDefinitions(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsCatalogsInDataManipulation(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsCatalogsInProcedureCalls(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsCatalogsInTableDefinitions(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsCatalogsInIndexDefinitions(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsCatalogsInPrivilegeDefinitions(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsPositionedDelete(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsPositionedUpdate(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsSelectForUpdate(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsStoredProcedures(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsSubqueriesInComparisons(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsSubqueriesInExists(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsSubqueriesInIns(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsSubqueriesInQuantifieds(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsCorrelatedSubqueries(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsUnion(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsUnionAll(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsOpenCursorsAcrossCommit(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsOpenCursorsAcrossRollback(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsOpenStatementsAcrossCommit(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsOpenStatementsAcrossRollback(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getMaxBinaryLiteralLength(): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxCharLiteralLength(): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxColumnNameLength(): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxColumnsInGroupBy(): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxColumnsInIndex(): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxColumnsInOrderBy(): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxColumnsInSelect(): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxColumnsInTable(): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxConnections(): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxCursorNameLength(): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxIndexLength(): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxSchemaNameLength(): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxProcedureNameLength(): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxCatalogNameLength(): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxRowSize(): Int {
        TODO("Not yet implemented")
    }

    override fun doesMaxRowSizeIncludeBlobs(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getMaxStatementLength(): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxStatements(): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxTableNameLength(): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxTablesInSelect(): Int {
        TODO("Not yet implemented")
    }

    override fun getMaxUserNameLength(): Int {
        TODO("Not yet implemented")
    }

    override fun getDefaultTransactionIsolation(): Int {
        TODO("Not yet implemented")
    }

    override fun supportsTransactions(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsTransactionIsolationLevel(level: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsDataDefinitionAndDataManipulationTransactions(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsDataManipulationTransactionsOnly(): Boolean {
        TODO("Not yet implemented")
    }

    override fun dataDefinitionCausesTransactionCommit(): Boolean {
        TODO("Not yet implemented")
    }

    override fun dataDefinitionIgnoredInTransactions(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getProcedures(catalog: String?, schemaPattern: String?, procedureNamePattern: String?): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getProcedureColumns(
        catalog: String?,
        schemaPattern: String?,
        procedureNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getTables(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        types: Array<out String>?
    ): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getSchemas(): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getSchemas(catalog: String?, schemaPattern: String?): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getCatalogs(): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getTableTypes(): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getColumns(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getColumnPrivileges(
        catalog: String?,
        schema: String?,
        table: String?,
        columnNamePattern: String?
    ): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getTablePrivileges(catalog: String?, schemaPattern: String?, tableNamePattern: String?): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getBestRowIdentifier(
        catalog: String?,
        schema: String?,
        table: String?,
        scope: Int,
        nullable: Boolean
    ): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getVersionColumns(catalog: String?, schema: String?, table: String?): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getPrimaryKeys(catalog: String?, schema: String?, table: String?): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getImportedKeys(catalog: String?, schema: String?, table: String?): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getExportedKeys(catalog: String?, schema: String?, table: String?): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getCrossReference(
        parentCatalog: String?,
        parentSchema: String?,
        parentTable: String?,
        foreignCatalog: String?,
        foreignSchema: String?,
        foreignTable: String?
    ): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getTypeInfo(): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getIndexInfo(
        catalog: String?,
        schema: String?,
        table: String?,
        unique: Boolean,
        approximate: Boolean
    ): ResultSet {
        TODO("Not yet implemented")
    }

    override fun supportsResultSetType(type: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsResultSetConcurrency(type: Int, concurrency: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun ownUpdatesAreVisible(type: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun ownDeletesAreVisible(type: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun ownInsertsAreVisible(type: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun othersUpdatesAreVisible(type: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun othersDeletesAreVisible(type: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun othersInsertsAreVisible(type: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun updatesAreDetected(type: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun deletesAreDetected(type: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun insertsAreDetected(type: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsBatchUpdates(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getUDTs(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?,
        types: IntArray?
    ): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getConnection(): Connection {
        TODO("Not yet implemented")
    }

    override fun supportsSavepoints(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsNamedParameters(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsMultipleOpenResults(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsGetGeneratedKeys(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getSuperTypes(catalog: String?, schemaPattern: String?, typeNamePattern: String?): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getSuperTables(catalog: String?, schemaPattern: String?, tableNamePattern: String?): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getAttributes(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?,
        attributeNamePattern: String?
    ): ResultSet {
        TODO("Not yet implemented")
    }

    override fun supportsResultSetHoldability(holdability: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun getResultSetHoldability(): Int {
        TODO("Not yet implemented")
    }

    override fun getDatabaseMajorVersion(): Int {
        return 10
    }

    override fun getDatabaseMinorVersion(): Int {
        return 6
    }

    override fun getJDBCMajorVersion(): Int {
        TODO("Not yet implemented")
    }

    override fun getJDBCMinorVersion(): Int {
        TODO("Not yet implemented")
    }

    override fun getSQLStateType(): Int {
        TODO("Not yet implemented")
    }

    override fun locatorsUpdateCopy(): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsStatementPooling(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getRowIdLifetime(): RowIdLifetime {
        TODO("Not yet implemented")
    }

    override fun supportsStoredFunctionsUsingCallSyntax(): Boolean {
        TODO("Not yet implemented")
    }

    override fun autoCommitFailureClosesAllResultSets(): Boolean {
        TODO("Not yet implemented")
    }

    override fun getClientInfoProperties(): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getFunctions(catalog: String?, schemaPattern: String?, functionNamePattern: String?): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getFunctionColumns(
        catalog: String?,
        schemaPattern: String?,
        functionNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        TODO("Not yet implemented")
    }

    override fun getPseudoColumns(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        TODO("Not yet implemented")
    }

    override fun generatedKeyAlwaysReturned(): Boolean {
        TODO("Not yet implemented")
    }
}