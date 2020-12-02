package no.anksoft.pginmem

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test

class TransactionsTest {
    private val datasource = PgInMemDatasource()

    @Test
    fun checkRollback() {
        datasource.connection.use { conn ->
            conn.autoCommit = false
            conn.prepareStatement(
                """
                create table mytable(
                    id text
                )
            """.trimIndent()
            ).use {
                it.executeUpdate()
            }

            conn.prepareStatement("""insert into mytable(id) values (?)""").use {
                it.setString(1, "secretkey")
                it.executeUpdate()
            }
            conn.rollback()
            conn.prepareStatement("select * from mytable").use { statement ->
                statement.executeQuery().use {
                    Assertions.assertThat(it.next()).isFalse()
                }
            }
        }
    }
}