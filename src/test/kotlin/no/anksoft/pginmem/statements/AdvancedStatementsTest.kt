package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
import org.assertj.core.api.Assertions
import org.jsonbuddy.JsonObject
import org.junit.jupiter.api.Test

class AdvancedStatementsTest {
    private val datasource = PgInMemDatasource()
    private val connection = datasource.connection

    @Test
    fun texToJson() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute(
                    """
                    create table mytable(
                        id  text,
                        info text,
                        email text
                        )
                """.trimIndent()
                )
            }
            conn.prepareStatement("insert into mytable(id,info) values (?,?)").use {
                it.setString(1, "mykey")
                it.setString(2,JsonObject().put("email","darth@deathstar.com").toJson())
                it.executeUpdate()
            }
            conn.prepareStatement("""
                update mytable
                   set email = info::json->>'email'
            """.trimIndent()).use {
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable").use {
                it.executeQuery().use {
                    Assertions.assertThat(it.next()).isTrue()
                    Assertions.assertThat(it.getString("email")).isEqualTo("darth@deathstar.com")
                }
            }
        }
    }

    @Test
    fun updateWithConstant() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute(
                    """
                    create table mytable(
                        id  text,
                        info text
                        )
                """.trimIndent()
                )
            }
            conn.prepareStatement("insert into mytable(id) values (?)").use {
                it.setString(1, "mykey")
                it.executeUpdate()
            }
            conn.prepareStatement("""
                update mytable
                   set info = 'Here is info'
            """.trimIndent()).use {
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable").use {
                it.executeQuery().use {
                    Assertions.assertThat(it.next()).isTrue()
                    Assertions.assertThat(it.getString("info")).isEqualTo("Here is info")
                }
            }
        }
    }

    @Test
    fun updateUsingIn() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute(
                    """
                    create table mytable(
                        id  text,
                        info text
                        )
                """.trimIndent()
                )
            }
            val insertSql = "insert into mytable(id) values (?)"
            conn.prepareStatement(insertSql).use {
                it.setString(1, "one")
                it.executeUpdate()
            }
            conn.prepareStatement(insertSql).use {
                it.setString(1, "two")
                it.executeUpdate()
            }
            conn.prepareStatement(insertSql).use {
                it.setString(1, "three")
                it.executeUpdate()
            }
            conn.prepareStatement("update mytable set info = 'hit' where id in ('one','two')").use {
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable where info = ?").use {
                it.setString(1,"hit")
                it.executeQuery().use {
                    Assertions.assertThat(it.next()).isTrue()
                    Assertions.assertThat(it.next()).isTrue()
                    Assertions.assertThat(it.next()).isFalse()
                }
            }
        }
    }
}