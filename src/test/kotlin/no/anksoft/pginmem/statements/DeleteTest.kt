package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test

class DeleteTest {
    private val datasource = PgInMemDatasource()
    private val connection = datasource.connection

    @Test
    fun shouldDelete() {
        connection.use { conn ->
            conn.prepareStatement("""
                    create table mytable(
                        id text
                    )
                """.trimIndent()).use {
                it.executeUpdate()
            }

            conn.prepareStatement("""insert into mytable(id) values (?)""").use {
                it.setString(1,"one")
                it.executeUpdate()
            }
            conn.prepareStatement("""insert into mytable(id) values (?)""").use {
                it.setString(1,"two")
                it.executeUpdate()
            }
            conn.prepareStatement("delete from mytable where id = ?").use {
                it.setString(1,"one")
                it.executeUpdate()
            }

            conn.prepareStatement("""select * from mytable""").use { ps ->
                ps.executeQuery().use {
                    Assertions.assertThat(it.next()).isTrue()
                    Assertions.assertThat(it.getString("id")).isEqualTo("two")
                    Assertions.assertThat(it.next()).isFalse()
                }
            }


        }
    }
}