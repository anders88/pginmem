package no.anksoft.pginmem

import java.sql.SQLException
import java.sql.Timestamp
import java.time.LocalDateTime

private fun splitStringToWords(sql:String):List<String> {
    val result:MutableList<String> = mutableListOf()
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
    while (index < trimmed.length) {
        val charAtPos:Char = trimmed[index]

        if (charAtPos.isWhitespace()) {
            if (index > previndex) {
                result.add(trimmed.substring(previndex,index))
            }
            index++
            previndex = index
            continue
        }
        if ("(),=<>".indexOf(charAtPos) != -1) {
            if (index > previndex) {
                result.add(trimmed.substring(previndex,index))
            }
            result.add(trimmed.substring(index,index+1))
            index++
            previndex=index
            continue
        }
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
                (aword == "now" && word(1) == "(" && word(2) == ")") -> {
                    addIndex(3)
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
}