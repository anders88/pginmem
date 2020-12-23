package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import java.sql.SQLException

class CreateTableTest {
    private val datasource = PgInMemDatasource()
    private val connection = datasource.connection

    @Test
    fun createWithDifferentTypes() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute("""
                create table mytable(
                    id  text,
                    num integer,
                    success boolean,
                    description text)
                """.trimIndent())
            }
            conn.prepareStatement("""insert into "public"."mytable"("id","success","num") values (?,?,?)""").use {
                it.setString(1,"myid")
                it.setBoolean(2,true)
                it.setInt(3,42)
                it.executeUpdate()
            }
            connection.prepareStatement("select * from mytable").executeQuery().use { resultSet ->
                assertThat(resultSet.next()).isTrue()
                assertThat(resultSet.getInt("num")).isEqualTo(42)
                assertThat(resultSet.getBoolean("success")).isTrue()
                assertThat(resultSet.getString("description")).isNull()
                assertThat(resultSet.next()).isFalse()
            }
        }
    }

    @Test
    fun createWithDefaultvalue() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute(
                    """
                create table mytable(
                    id  text,
                    created timestamp default now()
                    )
                """.trimIndent()
                )
            }
            conn.prepareStatement("insert into mytable(id) values (?)").use {
                it.setString(1,"myindex")
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable").use { statement ->
                statement.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getTimestamp("created")).isNotNull()
                }
            }
        }
    }

    @Test
    fun notNullFlag() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute("""
                    create table mytable(
                        id  text not null,
                        description text)
                """.trimIndent())
            }
            conn.prepareStatement("insert into mytable(id) values (?)").use {
                it.setString(1,"myid")
                it.executeUpdate()
            }
            try {
                conn.prepareStatement("insert into mytable(description) values (?)").use {
                    it.setString(1,"a text")
                    it.executeUpdate()
                }
                fail("Expected sqlexception not null")
            } catch (s:SQLException) { }
        }
    }

    @Test
    fun differentDefaults() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute(
                    """
                    create table mytable(
                        id  text,
                        currency text default 'NOK'::text,
                        isused boolean default true
                        )
                """.trimIndent()
                )
            }
            conn.prepareStatement("insert into mytable(id) values (?)").use {
                it.setString(1,"mykey")
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable").use { statement ->
                statement.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("mykey")
                    assertThat(it.getString("currency")).isEqualTo("NOK")
                    assertThat(it.getBoolean("isused")).isTrue()
                }
            }
        }
    }
}