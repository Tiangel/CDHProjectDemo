package com.cloudera.common.utils

case class PassengerFlowProfile(organizId: String, devices: Array[Device]) extends Serializable

case class Device(deviceId: String, countType: String) extends Serializable

case class PassengerFlowRecord(deviceId: String, cmdType: String, parentDeviceId: String, startTime: String, totalTime: Int, inNum: Int, outNum: Int) extends Serializable

case class PassengerMatchRecord(deviceId: String, startTime: String, inNum: Int, outNum: Int, inOrgId: String, outOrgId: String) extends Serializable
