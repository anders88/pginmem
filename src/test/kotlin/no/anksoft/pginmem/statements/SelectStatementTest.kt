package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class SelectStatementTest {
    private val datasource = PgInMemDatasource()
    private val connection = datasource.connection

    @Test
    fun selectWithLessThan() {
        connection.use { conn ->
            conn.createStatement().execute("""
                create table mytable(
                    id  integer,
                    description text)
            """.trimIndent())
            val insertSql = "insert into mytable(id,description) values (?,?)"
            conn.prepareStatement(insertSql).use {
                it.setInt(1,1)
                it.setString(2,"one")
                it.executeUpdate()
            }
            conn.prepareStatement(insertSql).use {
                it.setInt(1,2)
                it.setString(2,"two")
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable where id > ?").use { statement ->
                statement.setInt(1,1)
                statement.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("description")).isEqualTo("two")
                    assertThat(it.next()).isFalse()
                }
            }

        }
    }
}