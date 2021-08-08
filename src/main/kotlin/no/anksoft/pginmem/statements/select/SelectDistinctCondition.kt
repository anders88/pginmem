package no.anksoft.pginmem.statements.select

import no.anksoft.pginmem.DbTransaction
import no.anksoft.pginmem.Row
import no.anksoft.pginmem.ValueFromExpression
import no.anksoft.pginmem.values.CellValue

class SelectDistinctCondition constructor(val conditions:List<ValueFromExpression>) {
    fun doFilter(notFiltered: List<Pair<List<CellValue>, Row>>,dbTransaction: DbTransaction): List<List<CellValue>> {
        val mapval:MutableList<Pair<List<CellValue>,Int>> = mutableListOf()

        for (ind in notFiltered.indices) {
            val thisEntry = notFiltered[ind]
            val myKey:List<CellValue> = conditions.map { cond ->
                cond.genereateValue(dbTransaction,thisEntry.second)
            }
            if (mapval.firstOrNull { it.first == myKey } != null) {
                continue
            }
            mapval.add(Pair(myKey,ind))
        }

        return mapval.map { entry ->
            notFiltered[entry.second].first
        }
    }
}