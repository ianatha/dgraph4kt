package io.atha

import com.google.gson.Gson
import io.dgraph.DgraphClient
import io.grpc.StatusRuntimeException

class DgraphTransaction(val transaction: DgraphClient.Transaction) {
    val gson = Gson()

    fun queryWithVars(query: String, vars: Map<String, String>): List<Map<String, Any>> {
        val response = transaction.queryWithVars(query, vars)
        return (gson.fromJson(response.json.toStringUtf8(), Map::class.java) as Map<String, List<Map< String, Any>>>)["q"]!!
    }

    fun query(query: String) = queryWithVars(query, emptyMap())

    fun query(query: QueryTag) = query(query.toString())

    fun mutate(set: SetTag) {
        val response = transaction.mutate(set.toProtoBuf())
        set.mutationRefs.forEach {
            it.value.uid = response.uidsMap[it.key]
        }
    }
}

fun <T> DgraphClient.withTransaction(proc: DgraphTransaction.() -> T): T {
    return withTransaction(false, proc)
}

fun <T> DgraphClient.withReadOnlyTransaction(proc: DgraphTransaction.() -> T): T {
    return withTransaction(true, proc)
}

fun <T> DgraphClient.withTransaction(readOnly: Boolean, proc: DgraphTransaction.() -> T): T {
    var result: T
    val transaction = this.newTransaction()
    try {
        result = proc(DgraphTransaction(transaction))
        transaction.commit()
    } finally {
        try {
            transaction.discard()
        } catch (e: StatusRuntimeException) {
            // ignore
        }
    }
    return result
}
