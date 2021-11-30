package app.logflare.sql

import gudusoft.gsqlparser.nodes.TCTE
import gudusoft.gsqlparser.nodes.TObjectName
import gudusoft.gsqlparser.nodes.TParseTreeVisitor

class ParameterExtractor : TParseTreeVisitor() {

    val parameters: MutableSet<String> = mutableSetOf()

    override fun postVisit(node: TObjectName?) {
        if (node.toString().startsWith('@')) {
            parameters.add(node.toString().removePrefix("@"))
        }
        super.postVisit(node)
    }

    override fun postVisit(node: TCTE?) {
        node!!.subquery.acceptChildren(this)
        super.postVisit(node)
    }

}
