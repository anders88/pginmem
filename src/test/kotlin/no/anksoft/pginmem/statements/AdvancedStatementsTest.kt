package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
import org.assertj.core.api.Assertions
import org.jsonbuddy.JsonObject
import org.junit.jupiter.api.Test

class AdvancedStatementsTest {
    private val datasource = PgInMemDatasource()
    private val connection = datasource.connection

    
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
            """.trimIndent())
            conn.prepareStatement("select * from mytable").use {
                it.executeQuery().use {
                    Assertions.assertThat(it.next()).isTrue()
                    Assertions.assertThat(it.getString("email")).isEqualTo("darth@deathstar.com")
                }
            }
        }
    }
}