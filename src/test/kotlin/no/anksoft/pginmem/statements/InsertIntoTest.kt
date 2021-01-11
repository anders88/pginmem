package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.jsonbuddy.JsonObject
import org.junit.jupiter.api.Test
import java.util.*

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
            conn.createStatement().use {
                it.execute("create table myduplicate(id text,include text)")
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
            conn.prepareStatement("""
                insert into myduplicate(id,include) (select id,include from mytable)
            """.trimIndent()).use {
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

    @Test
    fun insertWithSelectUidCoalseceAndJson() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute("create table mytable(info text,deleted boolean)")
            }
            conn.createStatement().use {
                it.execute("create table yourtable(id text,readboolval boolean)")
            }
            conn.prepareStatement("insert into mytable(info) values (?)").use {
                it.setString(1,JsonObject().put("boolflag",false).toJson())
                it.executeUpdate()
            }
            conn.prepareStatement("insert into mytable(info,deleted) values (?,?)").use {
                it.setString(1,JsonObject().put("boolflag",false).toJson())
                it.setBoolean(2,true)
                it.executeUpdate()
            }
            conn.prepareStatement("""
                insert into yourtable(id,readboolval)
                select uuid_in(overlay(overlay(md5(random()::text || ':' || clock_timestamp()::text) placing '4' from 13) placing to_hex(floor(random()*(11-8+1) + 8)::int)::text from 17)::cstring),
                true from mytable where (info::json->>'boolflag')::BOOLEAN = false and coalesce(deleted,false) = false
            """.trimIndent()).use {
                it.executeUpdate()
            }
            conn.prepareStatement("select id from yourtable").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(UUID.fromString(it.getString("id"))).isNotNull()
                    assertThat(it.next()).isFalse()

                }
            }
        }

    }

}