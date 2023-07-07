object BinaryTreeMain extends App {


  val tree = new BinaryTree[Int, Boolean]()
  tree.insert(4, true)
  tree.insert(5, true)
  tree.insert(6, true)
  tree.insert(9, true)
  tree.insert(10, true)
  tree.insert(3, true)
  tree.insert(7, true)
  tree.delete(4)
  tree.printAll()


  class BinaryTree[E <% Ordered[E], V](var root: Option[TreeNode[E, V]] = None) {
    def insert(key: E, value: V) = {
      if (root.isDefined) root.get.insert(key, value)
      else root = Some(TreeNode(this, key, value))
    }

    def find(k: E) = if (root.isDefined) root.get.find(k) else None

    def delete(k: E) = if (root.isDefined) root.get.delete(k) else false

    def printAll() = {
      new LayerWiseIterator(root)
        .foreach(l => println(l.map(n => n.key).mkString(",")))

    }
  }

  class LayerWiseIterator[E <% Ordered[E], V](curNode: Option[TreeNode[E, V]]) extends Iterator[Seq[TreeNode[E, V]]] {
    var curLayer = collection.mutable.ListBuffer[TreeNode[E, V]]()
    if (curNode.isDefined) curLayer += curNode.get

    def hasNext() = !curLayer.isEmpty

    def next() = {
      val toReturn = curLayer.toSeq
      val nextLayer = collection.mutable.ListBuffer[TreeNode[E, V]]()
      nextLayer ++= curLayer.flatMap(n => Seq(n.left.getOrElse(null), n.right.getOrElse(null)).filter(_ != null))
      curLayer = nextLayer
      toReturn
    }
  }

  case class TreeNode[E <% Ordered[E], V](tree: BinaryTree[E, V],
                                          key: E,
                                          var value: V,
                                          var parent: Option[TreeNode[E, V]] = None,
                                          var left: Option[TreeNode[E, V]] = None,
                                          var right: Option[TreeNode[E, V]] = None) {

    def insert(newKey: E, newValue: V) = {
      val node = findClosestNode(newKey)
      if (node.key == newKey)
        node.value = newValue
      else if (newKey < node.key) {
        assert(node.left.isEmpty)
        node.left = Some(TreeNode[E, V](tree, newKey, newValue, Some(node)))
      } else {
        assert(node.right.isEmpty && newKey > node.key)
        node.right = Some(TreeNode[E, V](tree, newKey, newValue, Some(node)))
      }
    }

    def find(toFind: E): Option[V] = {
      val node = findClosestNode(toFind)
      if (node.key == toFind)
        Some(value)
      else
        None
    }

    def delete(toDelete: E) = {
      val node = findClosestNode(toDelete)
      if (node.key == toDelete) {
        node.removeSelf()
        true
      } else {
        false
      }
    }

    private def replaceChild(oldChild: TreeNode[E, V], newChild: Option[TreeNode[E, V]]) = {
      if (this.left.isDefined && this.left.get == oldChild)
        this.left = newChild
      else {
        assert(right.get == oldChild)
        this.right = right
      }
      if (newChild.isDefined) newChild.get.parent = Some(this)
    }

    private def removeSelf():Unit = {
      if (left.isEmpty && parent.isEmpty) {
        tree.root = right
        if (right.isDefined) right.get.parent = None
      } else if (right.isEmpty && parent.isEmpty) {
        tree.root = left
        if (left.isDefined) {
          left.get.parent = None
        }
      }
      else if (left.isEmpty) {
        this.parent.get.replaceChild(this, this.right)
      }
      else if (right.isEmpty) {
        parent.get.replaceChild(this, this.left)
      } else {
          println("asdugahsdfih")
        val replacement = left.get.findNodeOfMax
        replacement.removeSelf()
        if (parent.isDefined)
          parent.get.replaceChild(this, Some(replacement))
        else {
          assert(replacement.right.isEmpty)
          tree.root = Some(replacement)
          replacement.parent = None
        }
        replacement.right = this.right
        replacement.left = this.left
        if(left.isDefined) this.left.get.parent = Some(replacement)
        if(right.isDefined) this.right.get.parent = Some(replacement)
        println()
      }
    }

    private def findNodeOfMax: TreeNode[E, V] = {
      if (right.isEmpty)
        this
      else
        right.get.findNodeOfMax
    }

    private def findClosestNode(toFind: E): TreeNode[E, V] = {
      if (toFind < key) {
        if (left.isDefined) left.get.findClosestNode(toFind)
        else this
      } else if (toFind > key) {
        if (right.isDefined) right.get.findClosestNode(toFind)
        else this
      } else {
        this
      }
    }

  }
}