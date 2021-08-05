package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.data.Offset
import org.jsonbuddy.JsonObject
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import java.math.BigDecimal
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Timestamp
import java.time.LocalDateTime

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

    @Test
    fun selectFromSelectAsTable() {
        connection.use { conn ->
            conn.createStatement().execute("create table customer(id int,name text)")
            conn.createStatement().execute("create table sale(customerid int,amount numeric)")
            val insertIntoCustomer:(Pair<Int,String>)->Unit = { data ->
                conn.prepareStatement("insert into customer(id,name) values (?,?)").use { ps ->
                    ps.setInt(1,data.first)
                    ps.setString(2,data.second)
                    ps.executeUpdate()
                }
            }
            val insertIntoSale:(Pair<Int,BigDecimal>)->Unit = { data ->
                conn.prepareStatement("insert into sale(customerid,amount) values (?,?)").use { ps ->
                    ps.setInt(1,data.first)
                    ps.setBigDecimal(2,data.second)
                    ps.executeUpdate()
                }
            }

            insertIntoCustomer.invoke(Pair(1,"Luke"))
            insertIntoCustomer.invoke(Pair(2,"Darth"))

            insertIntoSale.invoke(Pair(1, BigDecimal.TEN))
            insertIntoSale.invoke(Pair(2, BigDecimal.TEN))
            insertIntoSale.invoke(Pair(2, BigDecimal(100.0)))

            val res:MutableList<Pair<String,BigDecimal>> = mutableListOf()

            val trans:(ResultSet)->Pair<String,BigDecimal>? = {
                if (it.next()) {
                    val name = it.getString("name")
                    val sale = it.getBigDecimal("totsale")
                    Pair(name,sale)
                } else null
            }

            conn.prepareStatement("""select c.name,s.totsale from customer c,(select customerid, sum(amount) as totsale from sale) as s where c.id = s.customerid""".trimMargin()).use { ps ->
                ps.executeQuery().use {
                    while (true) {
                        val x = trans.invoke(it) ?: break
                        res.add(x)
                    }
                }
            }
            assertThat(res).hasSize(2)
            assertThat(res.first { it.first == "Luke" }.second).isCloseTo(BigDecimal.TEN, Offset.offset(BigDecimal(0.001)))
            assertThat(res.first { it.first == "Darth" }.second).isCloseTo(BigDecimal(110.0), Offset.offset(BigDecimal(0.001)))
        }
    }

    @Test
    fun selectInTableWhereAndSelectInOuter() {
        connection.use { conn ->
            conn.createStatement().execute("create table mytable(id text,amount numeric,excludedat timestamp)")

            val insertAction:(Triple<String,BigDecimal,LocalDateTime?>)->Unit = { input ->
                conn.prepareStatement("insert into mytable(id,amount,excludedat) values (?,?,?)").use { ps ->
                    ps.setString(1,input.first)
                    ps.setBigDecimal(2,input.second)
                    ps.setTimestamp(3,input.third?.let { Timestamp.valueOf(it) })
                    ps.executeUpdate()
                }
            }
            insertAction.invoke(Triple("darth", BigDecimal.TEN,null))
            insertAction.invoke(Triple("darth", BigDecimal(100.0),null))
            insertAction.invoke(Triple("luke", BigDecimal(100.0), LocalDateTime.now()))

            conn.prepareStatement("""
                select name, amountsum from 
                (select id as name, sum(amount) as amountsum from mytable where excludedat is null) as amounts 
                where amountsum > 0               
            """.trimMargin()).executeQuery().use { rs ->
                assertThat(rs.next()).isTrue()
                assertThat(rs.getString("name")).isEqualTo("darth")
                assertThat(rs.getBigDecimal("amountsum")).isCloseTo(BigDecimal(110.0), Offset.offset(BigDecimal(0.0001)))
                assertThat(rs.next()).isFalse()
            }
        }

    }

    @Test
    fun selectWithNameFromInnerSelect() {
        connection.use { conn ->
            conn.createStatement().execute("create table mytable(name text, amount numeric)")
            val insact:(Pair<String,BigDecimal>)->Unit = { input ->
                conn.prepareStatement("insert into mytable(name,amount) values (?,?)").use { ps ->
                    ps.setString(1,input.first)
                    ps.setBigDecimal(2,input.second)
                    ps.executeUpdate()
                }
            }
            insact.invoke(Pair("luke", BigDecimal(10.0)))
            insact.invoke(Pair("luke",BigDecimal(32.0)))
            insact.invoke(Pair("darth",BigDecimal(100.0)))
            //conn.prepareStatement("select name,sum(amount) as sumam from mytable group by name").use { ps ->
            conn.prepareStatement("select * from (select name,sum(amount) as sumam from mytable group by name) as usedtable").use { ps ->
                ps.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("name")).isEqualTo("luke")
                    assertThat(it.getBigDecimal("sumam")).isCloseTo(BigDecimal(42.0), Offset.offset(BigDecimal(0.001)))
                    assertThat(it.next()).isTrue()
                    assertThat(it.next()).isFalse()
                }
            }

        }
    }

    @Test
    fun selectColoumnAggregate() {
        connection.use { conn ->
            conn.createStatement().execute("create table product(id integer,name text)")
            conn.createStatement().execute("create table sale(productid int,customername text)")
            val insProdAct:(Pair<Int,String>)->Unit = { input ->
                conn.prepareStatement("insert into product(id,name) values (?,?)").use {
                    it.setInt(1,input.first)
                    it.setString(2,input.second)
                    it.executeUpdate()
                }
            }
            val insSaleAct:(Pair<Int,String>)->Unit = { input ->
                conn.prepareStatement("insert into sale(productid,customername) values (?,?)").use {
                    it.setInt(1,input.first)
                    it.setString(2,input.second)
                    it.executeUpdate()
                }
            }
            insProdAct.invoke(Pair(1,"Lightsaber"))
            insProdAct.invoke(Pair(2,"Blaster"))
            insSaleAct.invoke(Pair(1,"Luke"))
            insSaleAct.invoke(Pair(2,"Luke"))
            insSaleAct.invoke(Pair(1,"Darth"))

            conn.prepareStatement("select count(distinct customername) from sale").use { ps ->
                ps.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getInt(1)).isEqualTo(2)
                    assertThat(it.next()).isFalse()

                }
            }

            val resset:MutableList<Pair<String,Int>> = mutableListOf()
            conn.prepareStatement("select p.name,(select count(distinct s.customername) from sale s where s.productid = p.id) as numcust from product p").use { ps ->
                ps.executeQuery().use {
                    while (it.next()) {
                        val name = it.getString("name")
                        val count = it.getInt("numcust")
                        resset.add(Pair(name,count))
                    }
                }
            }
            assertThat(resset).hasSize(2)
            assertThat(resset.first { it.first == "Lightsaber" }.second).isEqualTo(2)
            assertThat(resset.first { it.first == "Blaster" }.second).isEqualTo(1)
        }
    }



}