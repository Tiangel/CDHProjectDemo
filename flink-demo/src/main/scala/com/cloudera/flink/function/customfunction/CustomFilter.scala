package com.cloudera.flink.function.customfunction

import org.apache.flink.api.common.functions.FilterFunction

class CustomFilter(keyWord: String) extends FilterFunction[String] {
  override def filter(value: String): Boolean = {
    value.contains(keyWord)
  }
}
