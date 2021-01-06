package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class InsertIntoTest {
    private val datasource = PgInMemDatasource()
    private val connection = datasource.connection

    @Test
    fun insertWithConstants() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute(
                    """
                    create table mytable(
                        id  text,
                        info text,
                        price integer
                        )
                """.trimIndent()
                )
            }
            conn.prepareStatement("insert into mytable(id,info,price) values ('mykey','infoish',42)").use {
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("mykey")
                    assertThat(it.getString("info")).isEqualTo("infoish")
                    assertThat(it.getInt("price")).isEqualTo(42)
                }
            }
        }
    }

    @Test
    fun insertWithSelect() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute("create table mytable(id text,include text)")
            }
            conn.createStatement().use {
                it.execute("create table yourtable(id text)")
            }
            conn.prepareStatement("insert into mytable(id,include) values ('one','yes')").use {
                it.executeUpdate()
            }
            conn.prepareStatement("insert into mytable(id,include) values ('two','yes')").use {
                it.executeUpdate()
            }
            conn.prepareStatement("insert into mytable(id,include) values ('three','no')").use {
                it.executeUpdate()
            }
            conn.prepareStatement("""
                insert into yourtable(id) (select id from mytable where include = ?)
            """.trimIndent()).use {
                it.setString(1,"yes")
                it.executeUpdate()
            }
            conn.prepareStatement("select * from yourtable").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("one")
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("two")
                    assertThat(it.next()).isFalse()
                }
            }
        }
    }
}