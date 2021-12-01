package app.logflare.sql

import gudusoft.gsqlparser.nodes.*

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

    override fun postVisit(node: TFunctionCall?) {
        // GSP doesn't iterate over `args` which seem to contain parameters in our case
        node!!.args.acceptChildren(this)
        super.postVisit(node)
    }

}
