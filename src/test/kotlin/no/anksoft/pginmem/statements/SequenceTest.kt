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
            conn.createStatement().execute("ALTER SEQUENCE numgenerator RESTART")
            assertThat(readSeqVal(conn)).isEqualTo(1L)
        }

    }

    private fun readSeqVal(conn: Connection):Long {
        return conn.prepareStatement("select nextval('numGenerator')").use { ps ->
            ps.executeQuery().use {
                assertThat(it.next()).isTrue()
                it.getLong(1)
            }
        }
    }

    @Test
    fun seqenceAsDefault() {
        connection.use { conn ->
            conn.createStatement().execute("""
                create sequence mysequence
            """.trimIndent())
            conn.createStatement().execute("""
                create table mytable(
                    id integer default nextval('mysequence'),
                    description text)
            """.trimIndent())
            val insertSql = "insert into mytable(description) values (?)"
            conn.prepareStatement(insertSql).use {
                it.setString(1,"one")
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable").use { statement ->
                statement.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getInt("id")).isGreaterThan(0)
                }
            }
        }
    }

    @Test
    fun serialType() {
        connection.use { conn ->
            conn.createStatement().execute("""
                create table mytable(
                    id SERIAL PRIMARY KEY,
                    description text)
            """.trimIndent())
            val insertSql = "insert into mytable(description) values (?)"
            conn.prepareStatement(insertSql).use {
                it.setString(1,"one")
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable").use { statement ->
                statement.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getInt("id")).isGreaterThan(0)
                }
            }
            conn.prepareStatement("select * from mytable").use { statement ->
                statement.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getInt("id")).isGreaterThan(0)
                }
            }
        }
    }
}