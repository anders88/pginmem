package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.sql.Connection

class SequenceTest {
    private val datasource = PgInMemDatasource()
    private val connection = datasource.connection

    @Test
    fun createAndUseSquence() {
        connection.use { conn ->
            conn.createStatement().execute("create sequence numgenerator")
            assertThat(readSeqVal(conn)).isEqualTo(1L)
            assertThat(readSeqVal(conn)).isEqualTo(2L)
        }

    }

    private fun readSeqVal(conn: Connection):Long {
        return conn.prepareStatement("select nextval('numgenerator')").use { ps ->
            ps.executeQuery().use {
                assertThat(it.next()).isTrue()
                it.getLong(1)
            }
        }
    }
}