package com.cloudera

class SecondarySortKey(val first: Double, val second: Double) extends Ordered[SecondarySortKey] with Serializable {
  override def compare(that: SecondarySortKey): Int = {
    // 既然是二次排序，那么首先要判断第一个排序字段是否相等，如果不相等，就直接排序
    if (this.first - that.first != 0) {
      (this.first - that.first).toInt
    } else {
      //如果第一个字段相等，则比较第二个字段，若想实现多次排序，也可以按照这个模式继
      //续比较下去
      if (this.second - that.second > 0) {
        Math.ceil(this.second - that.second).toInt
      } else if (this.second - that.second < 0) {
        Math.floor(this.second - that.second).toInt
      } else {
        (this.second - that.second).toInt
      }
    }
  }
}
