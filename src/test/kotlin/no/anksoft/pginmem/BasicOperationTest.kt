package no.anksoft.pginmem

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import javax.sql.DataSource

class BasicOperationTest {
    private val datasource = PgInMemDatasource()
    private val connection = datasource.connection

    @Test
    fun createTableTestInsertReadTest() {
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

    @Test
    fun selectWithWhere() {
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
            conn.prepareStatement("""insert into mytable ( id ) values ( ? )""").use {
                it.setString(1,"anotherKey")
                it.executeUpdate()
            }

            conn.prepareStatement("select * from mytable where id = ?").use { statement ->
                statement.setString(1,"secretkey")
                statement.executeQuery().use {
                    Assertions.assertThat(it.next()).isTrue()
                    val res = it.getString("id")
                    Assertions.assertThat(res).isEqualTo("secretkey")
                    Assertions.assertThat(it.next()).isFalse()
                }
            }
        }

    }

}