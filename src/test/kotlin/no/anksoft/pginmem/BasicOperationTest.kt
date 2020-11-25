package no.anksoft.pginmem

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import javax.sql.DataSource

class BasicOperationTest {

    @Test
    fun createTableTestInsertReadTest() {
        val datasource = PgInMemDatasource()
        val connection = datasource.connection

        connection.use { conn ->
            conn.prepareStatement("""
                create table mytable (
                    id text
                )
            """.trimIndent()).use {
                it.executeUpdate()
            }

            conn.prepareStatement("""insert into mytable ( id ) values ( ? )""").use {
                it.setString(1,"secretkey")
                it.executeUpdate()
            }

            val res:String? = conn.prepareStatement("select * from mytable").use { statement ->
                statement.executeQuery().use {
                    Assertions.assertThat(it.next()).isTrue()
                    it.getString("id")
                }
            }
            Assertions.assertThat(res).isEqualTo("secretkey")
        }

    }
}