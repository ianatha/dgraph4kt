package io.atha

import io.dgraph.DgraphProto

interface Element {
    fun render(builder: StringBuilder, indent: String)
}

@DgraphTagMarker
abstract class Tag : Element {
    override fun toString(): String {
        val builder = StringBuilder()
        render(builder, "")
        return builder.toString()
    }
}

class AttrTag(val attr: String) : TagWithChildren<AttrTag>() {
    fun child(n: String): AttrTag = AttrTag(n)

    var _alias = ""

    override fun render(builder: StringBuilder, indent: String) {
        val rendered_alias = if (_alias.isEmpty()) {
            ""
        } else {
            "$_alias : "
        }
        builder.append("$indent$rendered_alias$attr")
        if (children.isEmpty()) {
            builder.append("\n")
        } else {
            builder.append(" {\n")
            children.forEach { it.render(builder, "$indent  ") }
            builder.append("$indent}\n")
        }
    }

    infix fun alias(alias: String): AttrTag {
        this._alias = alias
        return this
    }

    infix fun x(init: AttrTag.() -> Unit): AttrTag {
        this.init()
        return this
    }

    operator fun String.unaryPlus(): AttrTag {
        val result = child(this)
        children.add(result)
        return result
    }
}


class FuncTag(val fn: String, val attr: String, val value: String? = null) : Tag() {
    override fun render(builder: StringBuilder, indent: String) {
        if (value == null) {
            builder.append("""$fn($attr)""")
        } else {
            builder.append("""$fn($attr, "$value")""")
        }
    }
}

abstract class TagWithChildren<ChildrenType : Tag> : Tag() {
    val children = arrayListOf<Tag>()
}

@DslMarker
annotation class DgraphTagMarker

class SetTag : TagWithChildren<MutationInsutrctionTag>() {
    var mutationRefs: MutableMap<String, MutationRef> = mutableMapOf()

    override fun render(builder: StringBuilder, indent: String) {
        builder.append("$indent{ set {\n")
        children.forEach {
            it.render(builder, "$indent  ")
        }
        builder.append("$indent} }")
    }

    fun toProtoBuf(): DgraphProto.Mutation {
        val builder = DgraphProto.Mutation.newBuilder()
        (children as List<MutationInsutrctionTag>).forEach {
            builder.addSet(it.toProtoBuf()).build()
        }

        return builder.build()
    }

    fun nq(subject: MutationRef, predicate: String, obj: String): MutationInsutrctionTag {
        if (subject.id == null) {
            subject.id = "%04d".format(mutationRefs.size)
        }
        mutationRefs.set(subject.id!!, subject)

        val result = MutationInsutrctionTag(subject, predicate, obj)
        children.add(result)
        return result
    }
}

class MutationRef {
    constructor()

    constructor(id: String) {
        this.id = id
    }

    var id: String? = null

    var uid: String? = null

    override fun toString(): String {
        return if (uid != null) {
            "<$uid>"
        } else if (id != null) {
            "_:$id"
        } else {
            "<ref:?>"
        }
    }
}

class MutationInsutrctionTag(val subject: MutationRef, val predicate: String, val obj: String) : Tag() {
    override fun render(builder: StringBuilder, indent: String) {
        builder.append("$indent$subject $predicate $obj .\n")
    }

    fun toProtoBuf(): DgraphProto.NQuad {
        return DgraphProto.NQuad.newBuilder()
                .setSubject("_:${subject.id}")
                .setPredicate(predicate)
                .setObjectValue(DgraphProto.Value.newBuilder().setStrVal(obj).build())
                .build()
    }
}

class QueryTag(val name: String, val func: FuncTag, val first: Int? = null, val filter: FuncTag? = null) : TagWithChildren<AttrTag>() {
    fun child(name: String): AttrTag = AttrTag(name)

    override fun render(builder: StringBuilder, indent: String) {
        builder.append("${indent}{ $name(")
        builder.append("func: ")
        func.render(builder, "")
        if (first != null) {
            builder.append(", first: ").append(first)
        }
        builder.append(") ")
        if (filter != null) {
            builder.append("@filter(")
            filter.render(builder, "")
            builder.append(") ")
        }
        builder.append("{\n")
        children.forEach {
            it.render(builder, "$indent  ")
        }
        builder.append("$indent} }")
    }

    operator fun String.unaryPlus(): AttrTag {
        val result = child(this)
        children.add(result)
        return result
    }

    fun explore(): QueryTag {
        children.add(child("uid"))
        children.add(child("_predicate_"))
        children.add(child("expand(_all_)"))
        return this
    }
}

fun q(func: FuncTag, first: Int? = null, filter: FuncTag? = null, init: (QueryTag.() -> Unit)? = null): QueryTag {
    val result = QueryTag("q", func, first, filter)
    if (init != null) {
        result.init()
    }
    return result
}

fun set(init: SetTag.() -> Unit): SetTag {
    val result = SetTag()
    result.init()
    return result
}

fun has(field: String) = FuncTag("has", field)

fun uid(value: String) = FuncTag("uid", value)

fun eq(field: String, value: String) = FuncTag("eq", field, value)

fun anyofterms(field: String, value: String) = FuncTag("anyofterms", field, value)