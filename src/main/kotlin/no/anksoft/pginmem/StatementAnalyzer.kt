package no.anksoft.pginmem

import no.anksoft.pginmem.statements.select.SelectResultSet
import org.jsonbuddy.JsonObject
import org.jsonbuddy.parse.JsonParser
import java.sql.SQLException
import java.sql.Timestamp
import java.time.LocalDateTime

private fun splitStringToWords(sql:String):List<String> {
    val result:MutableList<String> = mutableListOf()
    // Set to lovercase if not quoted
    val toTrimmed = StringBuilder()
    var inQuote = false
    for (char in sql.trim()) {
        if (char == '\'') {
            inQuote = !inQuote
        }
        toTrimmed.append(if (inQuote) char else char.toLowerCase())
    }
    val trimmed = toTrimmed.toString()
    var index = 0
    var previndex = 0
    var inSpecialCaseSequence:Boolean = false
    inQuote = false
    while (index < trimmed.length) {
        val charAtPos:Char = trimmed[index]
        if (inQuote) {
            index++
            if (charAtPos == '\'') {
                result.add(trimmed.substring(previndex, index))
                previndex = index
                inQuote = false
            }
            continue
        }
        if (charAtPos == '\'') {
            if (index > previndex) {
                result.add(trimmed.substring(previndex, index))
            }
            previndex = index
            index++
            inQuote = true
            continue
        }

        if (charAtPos.isWhitespace()) {
            inSpecialCaseSequence = false
            if (index > previndex) {
                result.add(trimmed.substring(previndex,index))
            }
            index++
            previndex = index
            continue
        }
        if (charAtPos == ',') {
            if (index > previndex) {
                result.add(trimmed.substring(previndex,index))
            }
            result.add(",")
            index++
            previndex = index
            inSpecialCaseSequence = false
            continue
        }
        if ("()=<>:-".indexOf(charAtPos) != -1) {
            if (index > previndex && !inSpecialCaseSequence) {
                result.add(trimmed.substring(previndex,index))
                previndex = index
            }
            inSpecialCaseSequence = true;
            index++
            continue
        }
        if (inSpecialCaseSequence && index > previndex) {
            result.add(trimmed.substring(previndex,index))
            previndex = index
        }
        inSpecialCaseSequence = false
        index++
    }
    if (index > previndex) {
        result.add(trimmed.substring(previndex,index))
    }
    return result
}

class ValueFromExpression(val valuegen:((Pair<DbTransaction,Row?>)->Any?),val column: Column?)


class StatementAnalyzer {
    private val words:List<String>
    private var currentIndex:Int = 0

    constructor(sql:String) {
        this.words = splitStringToWords(sql)
    }

    private constructor(words:List<String>) {
        this.words = words
    }


    fun extractSelect(preappend:List<String>):StatementAnalyzer {
        val fromIndex = words.indexOf("from")
        val whereIndex = words.indexOf("where")

        if (fromIndex == -1 && whereIndex == -1) {
            return StatementAnalyzer(preappend)
        }
        if (fromIndex == -1 && whereIndex != -1) {
            val newWords:List<String> = preappend + words.subList(whereIndex,words.size)
            return StatementAnalyzer(newWords)
        }
        val newWords:List<String> = preappend + listOf(",") + words.subList(fromIndex,words.size)
        return StatementAnalyzer(newWords)
    }

    fun word(indexOffset:Int=0):String? {
        val readAt = currentIndex+indexOffset
        if (readAt < 0 || readAt >= words.size) {
            return null
        }
        return words[readAt]
    }

    fun addIndex(delta:Int=1):StatementAnalyzer {
        currentIndex+=delta
        return this
    }

    fun setIndex(index:Int):StatementAnalyzer {
        currentIndex = index
        return this
    }

    fun indexOf(word:String):Int {
        var resind = 0
        while (currentIndex+resind < words.size) {
            if (words[currentIndex+resind] == word) {
                return resind
            }
            resind++
        }
        return -1
    }

    fun wordAt(givenIndex:Int):String? = if (givenIndex+currentIndex >= 0 && givenIndex+currentIndex < words.size) words[givenIndex+currentIndex] else null

    val size: Int
        get() = words.size

    fun readValueFromExpression(dbTransaction: DbTransaction,tables: Map<String,Table>):ValueFromExpression {
        val aword:String = word()?:throw SQLException("Unexpected end of statement")
        when {
                (aword == "now" && word(1) == "()") -> {
                    addIndex(2)
                    return ValueFromExpression({ Timestamp.valueOf(LocalDateTime.now()) },null)
                }
            (aword == "nextval" && word(1) == "(" && word(3) == ")") -> {
                val seqnamestr = word(2)
                if (!(seqnamestr?.startsWith("'") == true && seqnamestr.endsWith("'") == true)) {
                    throw SQLException("Expected sequence name in nextval")
                }
                addIndex(4)
                val seqname = seqnamestr.substring(1,seqnamestr.length-1)
                dbTransaction.sequence(seqname)
                return ValueFromExpression({ it.first.sequence(seqname).nextVal() },null)

            }
            ("true" == aword) -> return ValueFromExpression({ true },null)
            ("false" == aword) -> return ValueFromExpression({ false },null)
            (aword.toLongOrNull() != null) -> return ValueFromExpression({ aword.toLong()},null)
            (aword.toBigDecimalOrNull() != null) -> return ValueFromExpression({ aword.toBigDecimal() },null)
            aword.startsWith("'") -> {
                val end = aword.indexOf("'",1)
                if (end == -1) {
                    throw SQLException("Illegal text $aword")
                }
                val text = aword.substring(1,end)
                return ValueFromExpression({ text },null)
            }
            else -> return readColumnValue(tables,aword)
        }
    }

    private fun readColumnValue(tables: Map<String, Table>, aword: String):ValueFromExpression {
        val column: Column = findColumnFromIdentifier(aword, tables)
        val valuegen:((Pair<DbTransaction,Row?>)->Any?) = { it.second?.cells?.firstOrNull { it.column == column }?.value }
        return ValueFromExpression(valuegen,column)
    }

    fun findColumnFromIdentifier(
        aword: String,
        tables: Map<String, Table>
    ): Column {
        val ind = aword.indexOf(".")
        val column: Column = if (ind == -1) {
            tables.values.map { it.findColumn(aword) }.filterNotNull().firstOrNull()
                ?: throw SQLException("Unknown column $aword")
        } else {
            var tablename = aword.substring(0, ind)
            if (tablename.startsWith("\"") && tablename.endsWith("\"")) {
                tablename = tablename.substring(1, tablename.length - 1)
            }
            val table: Table = tables[tablename] ?: throw SQLException("Unknown table $tablename")
            val colname = aword.substring(ind + 1)
            table.findColumn(colname) ?: throw SQLException("Unknown column $colname")
        }
        return column
    }

    fun readValueOnRow():(SelectResultSet)->Any? {
        val colname = word()?:throw SQLException("Unekpected end of statement")
        addIndex()
        if (colname.startsWith("'") && colname.endsWith("'")) {
            return ConstantValue(colname.substring(1,colname.length-1))
        }
        val resultTransformer:((Any?)->Any?)? = if (word() == "::") {
            if (addIndex().word() != "json") {
                throw SQLException("Unsupported konversion ${word()}")
            }
            if (addIndex().word() != "->>") {
                throw SQLException("Expected ->>")
            }
            addIndex()
            val jsonkey = word()
            if (!(jsonkey?.startsWith("'") == true && word()?.endsWith("'") == true)) {
                throw SQLException("Expected jsonkey got $jsonkey")
            }
            addIndex()
            JsonTransformer(jsonkey.substring(1,jsonkey.length-1))
        }  else null
        return ReadFromRow(colname,resultTransformer)
    }
}

private class ReadFromRow(val collabel:String,val resultTransformer:((Any?)->Any?)?):(SelectResultSet)->Any? {
    override fun invoke(row: SelectResultSet): Any? {
        var value:Any? = row.readCell(collabel)
        if (resultTransformer != null) {
            value = resultTransformer.invoke(value)
        }
        return value
    }
}

private class JsonTransformer(val key:String):(Any?)->Any? {
    override fun invoke(inputValue: Any?): Any? {
        if (inputValue !is String) {
            return null
        }
        val jsonNode = JsonParser.parse(inputValue)
        if (jsonNode !is JsonObject) {
            throw SQLException("Expected jsonobject got $inputValue")
        }
        return jsonNode.stringValue(key).orElse(null)
    }
}

private class ConstantValue(val value:Any?):(SelectResultSet)->Any? {
    override fun invoke(selectResultSet:SelectResultSet): Any? {
        return value
    }

}