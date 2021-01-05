package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.jsonbuddy.JsonObject
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import java.sql.SQLException

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
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("email")).isEqualTo("darth@deathstar.com")
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
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("info")).isEqualTo("Here is info")
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
                    assertThat(it.next()).isTrue()
                    assertThat(it.next()).isTrue()
                    assertThat(it.next()).isFalse()
                }
            }
        }
    }

    @Test
    fun failedInsertsTest() {
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
            try {
                conn.prepareStatement("insert into mytable(id) valx (?)")
                fail("Expected sql exception")
            } catch (e: SQLException) {
            }
        }
    }

    @Test
    fun updateFromSelect() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute(
                    """
                    create table onetable(
                        id  text,
                        info text
                        )
                """.trimIndent()
                )
            }
            conn.createStatement().use {
                it.execute(
                    """
                    create table twotable(
                        id  text,
                        info text
                        )
                """.trimIndent()
                )
            }
            conn.prepareStatement("insert into onetable(id,info) values (?,?)").use {
                it.setString(1,"one")
                it.setString(2,"oneinfo")
                it.executeUpdate()
            }
            conn.prepareStatement("insert into onetable(id,info) values (?,?)").use {
                it.setString(1,"two")
                it.setString(2,"twoinfo")
                it.executeUpdate()
            }
            conn.prepareStatement("insert into twotable(id) values (?)").use {
                it.setString(1,"one")
                it.executeUpdate()
            }
            conn.prepareStatement("insert into twotable(id) values (?)").use {
                it.setString(1,"two")
                it.executeUpdate()
            }
            conn.prepareStatement("""
                update twotable set info = a.info from onetable a
                where twotable.id = a.id
            """.trimIndent()).use {
                it.executeUpdate()
            }
            conn.prepareStatement("select * from twotable order by id").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("one")
                    assertThat(it.getString("info")).isEqualTo("oneinfo")
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("two")
                    assertThat(it.getString("info")).isEqualTo("twoinfo")
                    assertThat(it.next()).isFalse()
                }
            }
        }
    }

}