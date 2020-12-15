package no.anksoft.pginmem

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import java.sql.Timestamp
import java.time.LocalDateTime

class BasicOperationTest {
    private val datasource = PgInMemDatasource()
    private val connection = datasource.connection

    @Test
    fun createTableTestInsertReadTest() {
        connection.use { conn ->
            conn.prepareStatement("""
                create table mytable(
                    id text
                )
            """.trimIndent()).use {
                it.executeUpdate()
            }

            conn.prepareStatement("""insert into mytable(id) values (?)""").use {
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

            conn.prepareStatement("""insert into mytable (id) values (?)""").use {
                it.setString(1,"secretkey")
                it.executeUpdate()
            }
            conn.prepareStatement("""insert into mytable (id) values (?)""").use {
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

    @Test
    fun multipleDifferentColumnsWithUpdate() {
        val aDate:LocalDateTime = LocalDateTime.now()
        connection.use { conn ->
            conn.prepareStatement("""
                create table mytable (
                    id text,
                    name text,
                    created timestamp
                )
            """.trimIndent()).use {
                it.executeUpdate()
            }

            conn.prepareStatement("""insert into mytable (id,name,created) values (?,?,?)""").use {
                it.setString(1,"secretkey")
                it.setString(2,"Darth Vader")
                it.setTimestamp(3, Timestamp.valueOf(aDate))
                it.executeUpdate()
            }

            conn.prepareStatement("select * from mytable where id = ?").use { statement ->
                statement.setString(1,"secretkey")
                statement.executeQuery().use {
                    Assertions.assertThat(it.next()).isTrue()
                    val res = it.getString("id")
                    val readDate:LocalDateTime = it.getTimestamp("created").toLocalDateTime()
                    Assertions.assertThat(res).isEqualTo("secretkey")
                    Assertions.assertThat(readDate).isEqualTo(aDate)
                    Assertions.assertThat(it.getString("name")).isEqualTo("Darth Vader")
                    Assertions.assertThat(it.next()).isFalse()
                }
            }

            conn.prepareStatement("""update mytable set name= ? where id = ?""").use {
                it.setString(1,"Anakin Skywalker")
                it.setString(2,"secretkey")
                it.executeUpdate()
            }

            conn.prepareStatement("select * from mytable where id = ?").use { statement ->
                statement.setString(1,"secretkey")
                statement.executeQuery().use {
                    Assertions.assertThat(it.next()).isTrue()
                    val res = it.getString("id")
                    val readDate:LocalDateTime = it.getTimestamp("created").toLocalDateTime()
                    Assertions.assertThat(res).isEqualTo("secretkey")
                    Assertions.assertThat(readDate).isEqualTo(aDate)
                    Assertions.assertThat(it.getString("name")).isEqualTo("Anakin Skywalker")
                    Assertions.assertThat(it.next()).isFalse()
                }
            }

        }
    }


}