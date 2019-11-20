package accumulator

import java.util
import java.util.ArrayList

import org.apache.spark.util.{AccumulatorV2, CollectionAccumulator}

import scala.collection.mutable
import scala.reflect.internal.util.Collections

class MapAccumulator[T] extends AccumulatorV2[T,mutable.Map[String, T]]{
  private val _map: mutable.Map[String, T] =
    Collections.synchronized(mutable.Map.empty[String, T])

  override def isZero: Boolean = _map.isEmpty

  override def reset(): Unit = _map.clear()

  override def copy(): AccumulatorV2[T, mutable.Map[String, T]] = ???

  override def add(v: T): Unit = ???

  override def merge(other: AccumulatorV2[T, mutable.Map[String, T]]): Unit = other match {
    case o: MapAccumulator[T] => _map.++=(o.value)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: mutable.Map[String, T] = _map.synchronized {
    mutable.Map.canBuildFrom(_map.take(_map.size)).result()
  }
}
