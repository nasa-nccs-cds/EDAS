package nasa.nccs.utilities
import scala.collection.GenTraversableOnce
import scala.collection.mutable.ListBuffer

object DNodeRelation extends Enumeration { val Input, Product, Antecedent, Predecesor = Value }
object DNodeDirection extends Enumeration { val Post, Pre = Value }

object DAGNode {
  def sort[T <: DAGNode]( nodes: Seq[T] ): Seq[T] = nodes.sortWith( (n0,n1) => n1.hasPredecesor(n0) )
}

class DAGNode extends Loggable {
  type DNRelation = DNodeRelation.Value
  type DNDirection = DNodeDirection.Value

  private val _inputs = ListBuffer [DAGNode]()
  private val _products = ListBuffer [DAGNode]()

  def addInput(node: DAGNode ) = {
    assert( !(node eq this), " Error, Attempt to add DAG node to itself as child.")
    if( !hasInput(node) ) {
      assert(!(node.hasPredecesor(this)), " Error, Attempt to create a circular DAG graph.")
      _inputs += node;
      node._products += this;
    }
  }

  private def getRelatives( direction: DNDirection ): ListBuffer[DAGNode] = direction match {
    case DNodeDirection.Pre => _inputs
    case DNodeDirection.Post => _products
  }

  private def collectRelatives( relation: DNRelation ): ListBuffer[DAGNode] = relation match {
    case DNodeRelation.Input => _inputs
    case DNodeRelation.Product => _products
    case DNodeRelation.Antecedent => accumulate( DNodeDirection.Post )
    case DNodeRelation.Predecesor => accumulate( DNodeDirection.Pre )
  }

  private def accumulate( direction: DNDirection ): ListBuffer[DAGNode] =
    getRelatives(direction).foldLeft( ListBuffer[DAGNode]() ) {
      (results,node) => {
        results += node;
        results ++= node.accumulate( direction )
      }
    }

  private def accumulate( direction: DNDirection, filter: DAGNode => Boolean ): ListBuffer[DAGNode] =
    getRelatives(direction).foldLeft( ListBuffer[DAGNode]() ) {
      (results,node) => if( filter(node) ) {
        results += node;
        results ++= node.accumulate( direction )
      } else { results }
    }

  def find( relation: DNRelation, p: (DAGNode) ⇒ Boolean ): Option[DAGNode] = collectRelatives(relation).find( p )
  def filter( relation: DNRelation, p: (DAGNode) ⇒ Boolean ): List[DAGNode] = collectRelatives(relation).filter( p ).toList
  def filterNot( relation: DNRelation, p: (DAGNode) ⇒ Boolean ): List[DAGNode] = collectRelatives(relation).filterNot( p ).toList
  def exists( relation: DNRelation, p: (DAGNode) ⇒ Boolean ): Boolean = collectRelatives(relation).exists( p )
  def forall( relation: DNRelation, p: (DAGNode) ⇒ Boolean ): Boolean = collectRelatives(relation).forall( p )
  def foreach( relation: DNRelation, f: (DAGNode) ⇒ Unit ): Unit = collectRelatives(relation).foreach( f )
  def flatten( relation: DNRelation ): List[DAGNode] = collectRelatives(relation).toList
  def flatMap[B]( relation: DNRelation, f: (DAGNode) ⇒ GenTraversableOnce[B] ): List[B] = collectRelatives(relation).flatMap( f ).toList

  def hasPredecesor(dnode: DAGNode ): Boolean = exists( DNodeRelation.Predecesor, _ eq dnode )
  def hasAntecedent(dnode: DAGNode ): Boolean = exists( DNodeRelation.Antecedent, _ eq dnode )
  def hasInput(dnode: DAGNode ): Boolean = exists( DNodeRelation.Input, _ eq dnode )
  def hasProduct(dnode: DAGNode ): Boolean = exists( DNodeRelation.Product, _ eq dnode )
  def size( relation: DNRelation ): Int = collectRelatives(relation).size
  def isRoot: Boolean =  size( DNodeRelation.Product ) == 0
  def predecesors: ListBuffer[DAGNode] = accumulate( DNodeDirection.Pre )
  def predecesors(filter: DAGNode => Boolean): ListBuffer[DAGNode] = accumulate( DNodeDirection.Pre, filter )
}

class LabeledDAGNode(id: String) extends DAGNode with Loggable {
  override def toString: String = s"DAGNode[$id]"
}

class DAGNodeTest {
  val dn1 = new LabeledDAGNode("N1")
  val dn2 = new LabeledDAGNode("N2")
  val dn3 = new LabeledDAGNode("N3")
  val dn4 = new LabeledDAGNode("N4")

  dn1.addInput(dn2)
  dn2.addInput(dn3)
  dn2.addInput(dn4)

  println( dn1.size( DNodeRelation.Predecesor ) )
  println( dn2.size( DNodeRelation.Predecesor ) )
  println( dn4.size( DNodeRelation.Predecesor ) )
  println( dn4.size( DNodeRelation.Antecedent ) )
  println( dn4.hasAntecedent( dn1 ) )
  println( dn4.hasPredecesor( dn1 ) )
  println( dn1.hasAntecedent( dn4 ) )
  println( dn1.hasPredecesor( dn4 ) )

  println(  DAGNode.sort( List( dn4, dn2, dn3, dn1 ) ).mkString( ", ") )
}
