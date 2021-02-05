package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class UpdateStatementTest {
    private val datasource = PgInMemDatasource()
    private val connection = datasource.connection

    @Test
    fun insertTwoUpdateOne() {
        connection.use { conn ->
            conn.createStatement().execute("create table mytable(id text,description text)")

            conn.prepareStatement("insert into mytable(id) values(?)").use {
                it.setString(1,"a")
                it.executeUpdate()
            }
            conn.prepareStatement("insert into mytable(id) values(?)").use {
                it.setString(1,"b")
                it.executeUpdate()
            }

            conn.prepareStatement("update mytable set description = ? where id = ?").use {
                it.setString(1,"mydesc")
                it.setString(2,"a")
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.next()).isTrue()
                    assertThat(it.next()).isFalse()
                }
            }
        }
    }

    @Test
    fun updateWithBoolean() {
        connection.use { conn ->
            conn.createStatement().execute("create table mytable(id text,boolval boolean)")
            conn.prepareStatement("insert into mytable(id,boolval) values (?,?)").use {
                it.setString(1,"a")
                it.setBoolean(2,false)
                it.executeUpdate()
            }
            conn.prepareStatement("update mytable set boolval=true where id = ?").use {
                it.setString(1,"a")
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getBoolean("boolval")).isTrue()
                }
            }
        }
    }

    @Test
    fun updateMoreThanOnce() {
        connection.use {
            it.createStatement().execute(
                """
               create table mytable(id text, oneval text, twoval text)
                """.trimIndent()
            )
            it.prepareStatement("insert into mytable(id) values(?)").use {
                it.setString(1, "a")
                it.executeUpdate()
            }

            it.prepareStatement("update mytable set oneval = ? where id = ?").use {
                it.setString(1, "one")
                it.setString(2, "a")
                it.executeUpdate()
            }

            it.prepareStatement("update mytable set twoval = ? where id = ?").use {
                it.setString(1, "two")
                it.setString(2, "a")
                it.executeUpdate()
            }

            it.prepareStatement("select * from mytable").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.next()).isFalse()
                }
            }

        }
    }
}
