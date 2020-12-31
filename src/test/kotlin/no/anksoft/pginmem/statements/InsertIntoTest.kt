package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
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
        }
    }
}