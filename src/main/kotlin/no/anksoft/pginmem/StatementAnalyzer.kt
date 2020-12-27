package no.anksoft.pginmem

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
    while (index < trimmed.length) {
        val charAtPos:Char = trimmed[index]

        if (charAtPos.isWhitespace()) {
            inSpecialCaseSequence = false
            if (index > previndex) {
                result.add(trimmed.substring(previndex,index))
            }
            index++
            previndex = index
            continue
        }
        if ("(),=<>:-".indexOf(charAtPos) != -1) {
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


class StatementAnalyzer(val sql:String) {
    private val words:List<String> = splitStringToWords(sql)
    private var currentIndex:Int = 0

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

    fun indexOf(word:String) = words.indexOf(word)

    fun wordAt(givenIndex:Int):String? = if (givenIndex >= 0 && givenIndex < words.size) words[givenIndex] else null

    val size = words.size
    fun subList(fromIndex:Int,toIndex:Int) = words.subList(fromIndex,toIndex)

    fun readConstantValue():(()->Any?)? {
        val aword:String = word()?:return null
        when {
                (aword == "now" && word(1) == "()") -> {
                    addIndex(2)
                    return { Timestamp.valueOf(LocalDateTime.now()) }
                }
            ("true" == aword) -> return { true }
            ("false" == aword) -> return { false }
            (aword.toLongOrNull() != null) -> return { aword.toLong()}
            (aword.toBigDecimalOrNull() != null) -> return { aword.toBigDecimal() }
            aword.startsWith("'") -> {
                val end = aword.indexOf("'",1)
                if (end == -1) {
                    throw SQLException("Illegal text $aword")
                }
                val text = aword.substring(1,end)
                return { text }
            }
            else -> return null
        }
    }

    fun readValueOnRow(tables:List<Table>):(Row)->Any? {
        val colname = word()?:throw SQLException("Unekpected end of statement")
        val column:Column = tables.map { it.findColumn(colname)}
            .filterNotNull()
            .firstOrNull()?:throw SQLException("Unknown column $colname")
        addIndex()
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
        return ReadFromRow(column,resultTransformer)
    }
}

private class ReadFromRow(val column: Column,val resultTransformer:((Any?)->Any?)?):(Row)->Any? {
    override fun invoke(row: Row): Any? {
        val cell = row.cells.first { it.column == column }
        var value = cell.value
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