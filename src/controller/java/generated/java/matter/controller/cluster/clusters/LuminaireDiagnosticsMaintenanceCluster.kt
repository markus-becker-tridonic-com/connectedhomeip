/*
 *
 *    Copyright (c) 2023 Project CHIP Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package matter.controller.cluster.clusters

import java.util.logging.Level
import java.util.logging.Logger
import java.time.Duration
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.transform
import matter.controller.MatterController
import matter.controller.ReadRequest
import matter.controller.ReadData
import matter.controller.ReadFailure
import matter.controller.ReadResponse
import matter.controller.SubscribeRequest
import matter.controller.SubscriptionState
import matter.controller.ByteSubscriptionState
import matter.controller.ShortSubscriptionState
import matter.controller.IntSubscriptionState
import matter.controller.LongSubscriptionState
import matter.controller.FloatSubscriptionState
import matter.controller.DoubleSubscriptionState
import matter.controller.CharSubscriptionState
import matter.controller.BooleanSubscriptionState
import matter.controller.UByteSubscriptionState
import matter.controller.UShortSubscriptionState
import matter.controller.UIntSubscriptionState
import matter.controller.ULongSubscriptionState
import matter.controller.StringSubscriptionState
import matter.controller.ByteArraySubscriptionState
import matter.controller.WriteRequest
import matter.controller.WriteRequests
import matter.controller.WriteResponse
import matter.controller.AttributeWriteError
import matter.controller.InvokeRequest
import matter.controller.InvokeResponse
import matter.controller.model.AttributePath
import matter.controller.model.CommandPath
import matter.controller.cluster.structs.*
import matter.tlv.AnonymousTag
import matter.tlv.ContextSpecificTag
import matter.tlv.Tag
import matter.tlv.TlvParsingException
import matter.tlv.TlvReader
import matter.tlv.TlvWriter

class LuminaireDiagnosticsMaintenanceCluster(private val controller: MatterController, private val endpointId: UShort) {class GeneratedCommandListAttribute(
    val value: List<UInt>
  )

  sealed class GeneratedCommandListAttributeSubscriptionState {
    data class Success(
    val value: List<UInt>
    ) : GeneratedCommandListAttributeSubscriptionState()
    
    data class Error(val exception: Exception) : GeneratedCommandListAttributeSubscriptionState()

    object SubscriptionEstablished : GeneratedCommandListAttributeSubscriptionState()    
  }  
class AcceptedCommandListAttribute(
    val value: List<UInt>
  )

  sealed class AcceptedCommandListAttributeSubscriptionState {
    data class Success(
    val value: List<UInt>
    ) : AcceptedCommandListAttributeSubscriptionState()
    
    data class Error(val exception: Exception) : AcceptedCommandListAttributeSubscriptionState()

    object SubscriptionEstablished : AcceptedCommandListAttributeSubscriptionState()    
  }  
class EventListAttribute(
    val value: List<UInt>
  )

  sealed class EventListAttributeSubscriptionState {
    data class Success(
    val value: List<UInt>
    ) : EventListAttributeSubscriptionState()
    
    data class Error(val exception: Exception) : EventListAttributeSubscriptionState()

    object SubscriptionEstablished : EventListAttributeSubscriptionState()    
  }  
class AttributeListAttribute(
    val value: List<UInt>
  )

  sealed class AttributeListAttributeSubscriptionState {
    data class Success(
    val value: List<UInt>
    ) : AttributeListAttributeSubscriptionState()
    
    data class Error(val exception: Exception) : AttributeListAttributeSubscriptionState()

    object SubscriptionEstablished : AttributeListAttributeSubscriptionState()    
  }  
suspend fun readControlGearOperatingTimeAttribute(): UInt {val ATTRIBUTE_ID: UInt = 0u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgearoperatingtime attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UInt = tlvReader.getUInt(AnonymousTag)


    return decodedValue
  }

  suspend fun subscribeControlGearOperatingTimeAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UIntSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 0u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UIntSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgearoperatingtime attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UInt = tlvReader.getUInt(AnonymousTag)

          emit(UIntSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UIntSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readControlGearStartCounterAttribute(): UInt {val ATTRIBUTE_ID: UInt = 1u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgearstartcounter attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UInt = tlvReader.getUInt(AnonymousTag)


    return decodedValue
  }

  suspend fun subscribeControlGearStartCounterAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UIntSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 1u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UIntSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgearstartcounter attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UInt = tlvReader.getUInt(AnonymousTag)

          emit(UIntSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UIntSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readControlGearExternalSupplyVoltageAttribute(): UShort? {val ATTRIBUTE_ID: UInt = 2u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgearexternalsupplyvoltage attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UShort? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUShort(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeControlGearExternalSupplyVoltageAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UShortSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 2u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UShortSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgearexternalsupplyvoltage attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UShort? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUShort(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UShortSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UShortSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readControlGearExternalSupplyVoltageFrequencyAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 3u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgearexternalsupplyvoltagefrequency attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeControlGearExternalSupplyVoltageFrequencyAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 3u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgearexternalsupplyvoltagefrequency attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readControlGearPowerFactorAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 4u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgearpowerfactor attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeControlGearPowerFactorAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 4u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgearpowerfactor attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readControlGearOverallFailureConditionAttribute(): UByte {val ATTRIBUTE_ID: UInt = 5u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgearoverallfailurecondition attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte = tlvReader.getUByte(AnonymousTag)


    return decodedValue
  }

  suspend fun subscribeControlGearOverallFailureConditionAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 5u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgearoverallfailurecondition attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte = tlvReader.getUByte(AnonymousTag)

          emit(UByteSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readControlGearOverallFailureConditionCounterAttribute(): UByte {val ATTRIBUTE_ID: UInt = 6u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgearoverallfailureconditioncounter attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte = tlvReader.getUByte(AnonymousTag)


    return decodedValue
  }

  suspend fun subscribeControlGearOverallFailureConditionCounterAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 6u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgearoverallfailureconditioncounter attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte = tlvReader.getUByte(AnonymousTag)

          emit(UByteSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readControlGearExternalSupplyUndervoltageAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 7u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgearexternalsupplyundervoltage attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeControlGearExternalSupplyUndervoltageAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 7u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgearexternalsupplyundervoltage attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readControlGearExternalSupplyUndervoltageCounterAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 8u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgearexternalsupplyundervoltagecounter attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeControlGearExternalSupplyUndervoltageCounterAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 8u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgearexternalsupplyundervoltagecounter attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readControlGearExternalSupplyOvervoltageAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 9u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgearexternalsupplyovervoltage attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeControlGearExternalSupplyOvervoltageAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 9u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgearexternalsupplyovervoltage attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readControlGearExternalSupplyOvervoltageCounterAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 10u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgearexternalsupplyovervoltagecounter attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeControlGearExternalSupplyOvervoltageCounterAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 10u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgearexternalsupplyovervoltagecounter attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readControlGearOutputPowerLimitationAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 11u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgearoutputpowerlimitation attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeControlGearOutputPowerLimitationAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 11u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgearoutputpowerlimitation attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readControlGearOutputPowerLimitationCounterAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 12u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgearoutputpowerlimitationcounter attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeControlGearOutputPowerLimitationCounterAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 12u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgearoutputpowerlimitationcounter attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readControlGearThermalDeratingAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 13u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgearthermalderating attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeControlGearThermalDeratingAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 13u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgearthermalderating attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readControlGearThermalDeratingCounterAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 14u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgearthermalderatingcounter attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeControlGearThermalDeratingCounterAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 14u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgearthermalderatingcounter attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readControlGearThermalShutdownAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 15u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgearthermalshutdown attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeControlGearThermalShutdownAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 15u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgearthermalshutdown attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readControlGearThermalShutdownCounterAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 16u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgearthermalshutdowncounter attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeControlGearThermalShutdownCounterAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 16u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgearthermalshutdowncounter attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readControlGearTemperatureAttribute(): UByte {val ATTRIBUTE_ID: UInt = 17u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgeartemperature attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte = tlvReader.getUByte(AnonymousTag)


    return decodedValue
  }

  suspend fun subscribeControlGearTemperatureAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 17u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgeartemperature attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte = tlvReader.getUByte(AnonymousTag)

          emit(UByteSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readControlGearOutputCurrentPercentageAttribute(): UByte {val ATTRIBUTE_ID: UInt = 18u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Controlgearoutputcurrentpercentage attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte = tlvReader.getUByte(AnonymousTag)


    return decodedValue
  }

  suspend fun subscribeControlGearOutputCurrentPercentageAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 18u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Controlgearoutputcurrentpercentage attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte = tlvReader.getUByte(AnonymousTag)

          emit(UByteSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readLightSourceStartCounterResettableAttribute(): UInt {val ATTRIBUTE_ID: UInt = 32u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Lightsourcestartcounterresettable attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UInt = tlvReader.getUInt(AnonymousTag)


    return decodedValue
  }

  suspend fun writeLightSourceStartCounterResettableAttribute(
    value: UInt,
    timedWriteTimeout: Duration? = null) {
    val ATTRIBUTE_ID: UInt = 32u

    val tlvWriter = TlvWriter()
    tlvWriter.put(AnonymousTag, value) 

    val writeRequests: WriteRequests =
      WriteRequests(
        requests = listOf(
          WriteRequest(
            attributePath = AttributePath(
              endpointId,
              clusterId = CLUSTER_ID,
              attributeId = ATTRIBUTE_ID
            ),
            tlvPayload = tlvWriter.getEncoded()
          )
        ),
        timedRequest = timedWriteTimeout
      )

    val response: WriteResponse = controller.write(writeRequests)

    when (response) {
      is WriteResponse.Success -> {
        logger.log(Level.FINE, "Write command succeeded")
      }
      is WriteResponse.PartialWriteFailure -> {
        val aggregatedErrorMessage =
          response.failures.joinToString("\n") { failure ->
            "Error at ${failure.attributePath}: ${failure.ex.message}"
          }

        response.failures.forEach { failure ->
          logger.log(Level.WARNING, "Error at ${failure.attributePath}: ${failure.ex.message}")
        }

        throw IllegalStateException("Write command failed with errors: \n$aggregatedErrorMessage")
      }
    }   
  }

  suspend fun subscribeLightSourceStartCounterResettableAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UIntSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 32u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UIntSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Lightsourcestartcounterresettable attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UInt = tlvReader.getUInt(AnonymousTag)

          emit(UIntSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UIntSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readLightSourceStartCounterAttribute(): UInt {val ATTRIBUTE_ID: UInt = 33u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Lightsourcestartcounter attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UInt = tlvReader.getUInt(AnonymousTag)


    return decodedValue
  }

  suspend fun subscribeLightSourceStartCounterAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UIntSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 33u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UIntSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Lightsourcestartcounter attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UInt = tlvReader.getUInt(AnonymousTag)

          emit(UIntSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UIntSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readLightSourceOnTimeResettableAttribute(): UInt {val ATTRIBUTE_ID: UInt = 34u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Lightsourceontimeresettable attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UInt = tlvReader.getUInt(AnonymousTag)


    return decodedValue
  }

  suspend fun writeLightSourceOnTimeResettableAttribute(
    value: UInt,
    timedWriteTimeout: Duration? = null) {
    val ATTRIBUTE_ID: UInt = 34u

    val tlvWriter = TlvWriter()
    tlvWriter.put(AnonymousTag, value) 

    val writeRequests: WriteRequests =
      WriteRequests(
        requests = listOf(
          WriteRequest(
            attributePath = AttributePath(
              endpointId,
              clusterId = CLUSTER_ID,
              attributeId = ATTRIBUTE_ID
            ),
            tlvPayload = tlvWriter.getEncoded()
          )
        ),
        timedRequest = timedWriteTimeout
      )

    val response: WriteResponse = controller.write(writeRequests)

    when (response) {
      is WriteResponse.Success -> {
        logger.log(Level.FINE, "Write command succeeded")
      }
      is WriteResponse.PartialWriteFailure -> {
        val aggregatedErrorMessage =
          response.failures.joinToString("\n") { failure ->
            "Error at ${failure.attributePath}: ${failure.ex.message}"
          }

        response.failures.forEach { failure ->
          logger.log(Level.WARNING, "Error at ${failure.attributePath}: ${failure.ex.message}")
        }

        throw IllegalStateException("Write command failed with errors: \n$aggregatedErrorMessage")
      }
    }   
  }

  suspend fun subscribeLightSourceOnTimeResettableAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UIntSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 34u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UIntSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Lightsourceontimeresettable attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UInt = tlvReader.getUInt(AnonymousTag)

          emit(UIntSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UIntSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readLightSourceOnTimeAttribute(): UInt {val ATTRIBUTE_ID: UInt = 35u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Lightsourceontime attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UInt = tlvReader.getUInt(AnonymousTag)


    return decodedValue
  }

  suspend fun subscribeLightSourceOnTimeAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UIntSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 35u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UIntSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Lightsourceontime attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UInt = tlvReader.getUInt(AnonymousTag)

          emit(UIntSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UIntSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readLightSourceVoltageAttribute(): UShort {val ATTRIBUTE_ID: UInt = 36u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Lightsourcevoltage attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UShort = tlvReader.getUShort(AnonymousTag)


    return decodedValue
  }

  suspend fun subscribeLightSourceVoltageAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UShortSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 36u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UShortSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Lightsourcevoltage attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UShort = tlvReader.getUShort(AnonymousTag)

          emit(UShortSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UShortSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readLightSourceCurrentAttribute(): UShort {val ATTRIBUTE_ID: UInt = 37u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Lightsourcecurrent attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UShort = tlvReader.getUShort(AnonymousTag)


    return decodedValue
  }

  suspend fun subscribeLightSourceCurrentAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UShortSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 37u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UShortSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Lightsourcecurrent attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UShort = tlvReader.getUShort(AnonymousTag)

          emit(UShortSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UShortSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readLightSourceOverallFailureConditionAttribute(): UByte {val ATTRIBUTE_ID: UInt = 38u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Lightsourceoverallfailurecondition attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte = tlvReader.getUByte(AnonymousTag)


    return decodedValue
  }

  suspend fun subscribeLightSourceOverallFailureConditionAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 38u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Lightsourceoverallfailurecondition attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte = tlvReader.getUByte(AnonymousTag)

          emit(UByteSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readLightSourceOverallFailureConditionCounterAttribute(): UByte {val ATTRIBUTE_ID: UInt = 39u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Lightsourceoverallfailureconditioncounter attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte = tlvReader.getUByte(AnonymousTag)


    return decodedValue
  }

  suspend fun subscribeLightSourceOverallFailureConditionCounterAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 39u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Lightsourceoverallfailureconditioncounter attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte = tlvReader.getUByte(AnonymousTag)

          emit(UByteSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readLightSourceShortCircuitAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 40u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Lightsourceshortcircuit attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeLightSourceShortCircuitAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 40u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Lightsourceshortcircuit attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readLightSourceShortCircuitCounterAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 41u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Lightsourceshortcircuitcounter attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeLightSourceShortCircuitCounterAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 41u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Lightsourceshortcircuitcounter attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readLightSourceOpenCircuitAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 42u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Lightsourceopencircuit attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeLightSourceOpenCircuitAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 42u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Lightsourceopencircuit attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readLightSourceOpenCircuitCounterAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 43u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Lightsourceopencircuitcounter attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeLightSourceOpenCircuitCounterAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 43u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Lightsourceopencircuitcounter attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readLightSourceThermalDeratingAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 44u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Lightsourcethermalderating attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeLightSourceThermalDeratingAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 44u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Lightsourcethermalderating attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readLightSourceThermalDeratingCounterAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 45u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Lightsourcethermalderatingcounter attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeLightSourceThermalDeratingCounterAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 45u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Lightsourcethermalderatingcounter attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readLightSourceThermalShutdownAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 46u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Lightsourcethermalshutdown attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeLightSourceThermalShutdownAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 46u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Lightsourcethermalshutdown attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readLightSourceThermalShutdownCounterAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 47u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Lightsourcethermalshutdowncounter attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeLightSourceThermalShutdownCounterAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 47u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Lightsourcethermalshutdowncounter attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readLightSourceTemperatureAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 48u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Lightsourcetemperature attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeLightSourceTemperatureAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 48u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Lightsourcetemperature attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readRatedMedianUsefulLifeOfLuminaireAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 64u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Ratedmedianusefullifeofluminaire attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeRatedMedianUsefulLifeOfLuminaireAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 64u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Ratedmedianusefullifeofluminaire attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readInternalControlGearReferenceTemperatureAttribute(): UByte? {val ATTRIBUTE_ID: UInt = 65u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Internalcontrolgearreferencetemperature attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeInternalControlGearReferenceTemperatureAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UByteSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 65u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UByteSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Internalcontrolgearreferencetemperature attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UByte? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUByte(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UByteSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UByteSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readRatedMedianUsefulLightSourceStartsAttribute(): UShort? {val ATTRIBUTE_ID: UInt = 66u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Ratedmedianusefullightsourcestarts attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UShort? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUShort(AnonymousTag)
    } else {
      null
    }


    return decodedValue
  }

  suspend fun subscribeRatedMedianUsefulLightSourceStartsAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UShortSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 66u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UShortSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Ratedmedianusefullightsourcestarts attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UShort? = if (tlvReader.isNextTag(AnonymousTag)) {
      tlvReader.getUShort(AnonymousTag)
    } else {
      null
    }

          decodedValue?.let {
            emit(UShortSubscriptionState.Success(it))
          }
          
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UShortSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readGeneratedCommandListAttribute(): GeneratedCommandListAttribute {val ATTRIBUTE_ID: UInt = 65528u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Generatedcommandlist attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: List<UInt> = buildList<UInt> {
      tlvReader.enterArray(AnonymousTag)
      while(!tlvReader.isEndOfContainer()) {
        add(tlvReader.getUInt(AnonymousTag))
      }
      tlvReader.exitContainer()
    }


    return GeneratedCommandListAttribute(decodedValue)
  }

  suspend fun subscribeGeneratedCommandListAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<GeneratedCommandListAttributeSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 65528u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(GeneratedCommandListAttributeSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Generatedcommandlist attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: List<UInt> = buildList<UInt> {
      tlvReader.enterArray(AnonymousTag)
      while(!tlvReader.isEndOfContainer()) {
        add(tlvReader.getUInt(AnonymousTag))
      }
      tlvReader.exitContainer()
    }

          emit(GeneratedCommandListAttributeSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(GeneratedCommandListAttributeSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readAcceptedCommandListAttribute(): AcceptedCommandListAttribute {val ATTRIBUTE_ID: UInt = 65529u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Acceptedcommandlist attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: List<UInt> = buildList<UInt> {
      tlvReader.enterArray(AnonymousTag)
      while(!tlvReader.isEndOfContainer()) {
        add(tlvReader.getUInt(AnonymousTag))
      }
      tlvReader.exitContainer()
    }


    return AcceptedCommandListAttribute(decodedValue)
  }

  suspend fun subscribeAcceptedCommandListAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<AcceptedCommandListAttributeSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 65529u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(AcceptedCommandListAttributeSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Acceptedcommandlist attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: List<UInt> = buildList<UInt> {
      tlvReader.enterArray(AnonymousTag)
      while(!tlvReader.isEndOfContainer()) {
        add(tlvReader.getUInt(AnonymousTag))
      }
      tlvReader.exitContainer()
    }

          emit(AcceptedCommandListAttributeSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(AcceptedCommandListAttributeSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readEventListAttribute(): EventListAttribute {val ATTRIBUTE_ID: UInt = 65530u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Eventlist attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: List<UInt> = buildList<UInt> {
      tlvReader.enterArray(AnonymousTag)
      while(!tlvReader.isEndOfContainer()) {
        add(tlvReader.getUInt(AnonymousTag))
      }
      tlvReader.exitContainer()
    }


    return EventListAttribute(decodedValue)
  }

  suspend fun subscribeEventListAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<EventListAttributeSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 65530u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(EventListAttributeSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Eventlist attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: List<UInt> = buildList<UInt> {
      tlvReader.enterArray(AnonymousTag)
      while(!tlvReader.isEndOfContainer()) {
        add(tlvReader.getUInt(AnonymousTag))
      }
      tlvReader.exitContainer()
    }

          emit(EventListAttributeSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(EventListAttributeSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readAttributeListAttribute(): AttributeListAttribute {val ATTRIBUTE_ID: UInt = 65531u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Attributelist attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: List<UInt> = buildList<UInt> {
      tlvReader.enterArray(AnonymousTag)
      while(!tlvReader.isEndOfContainer()) {
        add(tlvReader.getUInt(AnonymousTag))
      }
      tlvReader.exitContainer()
    }


    return AttributeListAttribute(decodedValue)
  }

  suspend fun subscribeAttributeListAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<AttributeListAttributeSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 65531u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(AttributeListAttributeSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Attributelist attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: List<UInt> = buildList<UInt> {
      tlvReader.enterArray(AnonymousTag)
      while(!tlvReader.isEndOfContainer()) {
        add(tlvReader.getUInt(AnonymousTag))
      }
      tlvReader.exitContainer()
    }

          emit(AttributeListAttributeSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(AttributeListAttributeSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readFeatureMapAttribute(): UInt {val ATTRIBUTE_ID: UInt = 65532u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Featuremap attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UInt = tlvReader.getUInt(AnonymousTag)


    return decodedValue
  }

  suspend fun subscribeFeatureMapAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UIntSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 65532u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UIntSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Featuremap attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UInt = tlvReader.getUInt(AnonymousTag)

          emit(UIntSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UIntSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }
suspend fun readClusterRevisionAttribute(): UShort {val ATTRIBUTE_ID: UInt = 65533u

    val attributePath = AttributePath(
      endpointId = endpointId, 
      clusterId = CLUSTER_ID,
      attributeId = ATTRIBUTE_ID
    )

    val readRequest = ReadRequest(
      eventPaths = emptyList(),
      attributePaths = listOf(attributePath)
    )
    
    val response = controller.read(readRequest)

    if (response.successes.isEmpty()) {
      logger.log(Level.WARNING, "Read command failed")
      throw IllegalStateException("Read command failed with failures: ${response.failures}")     
    }    

    logger.log(Level.FINE, "Read command succeeded")

    val attributeData =
      response.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
        it.path.attributeId == ATTRIBUTE_ID
      }        
       
    requireNotNull(attributeData) { 
      "Clusterrevision attribute not found in response" 
    }

    // Decode the TLV data into the appropriate type
    val tlvReader = TlvReader(attributeData.data)
    val decodedValue: UShort = tlvReader.getUShort(AnonymousTag)


    return decodedValue
  }

  suspend fun subscribeClusterRevisionAttribute(
    minInterval: Int,
    maxInterval: Int
  ): Flow<UShortSubscriptionState> {
    val ATTRIBUTE_ID: UInt = 65533u
    val attributePaths = listOf(
      AttributePath(
          endpointId = endpointId,
          clusterId = CLUSTER_ID,
          attributeId = ATTRIBUTE_ID
      )
    )

    val subscribeRequest: SubscribeRequest = SubscribeRequest(
      eventPaths = emptyList(), 
      attributePaths = attributePaths, 
      minInterval = Duration.ofSeconds(minInterval.toLong()), 
      maxInterval = Duration.ofSeconds(maxInterval.toLong())
    )

    return controller.subscribe(subscribeRequest).transform { subscriptionState ->
      when (subscriptionState) {
        is SubscriptionState.SubscriptionErrorNotification -> {
          emit(UShortSubscriptionState.Error(Exception("Subscription terminated with error code: ${subscriptionState.terminationCause}")))
        }
        is SubscriptionState.NodeStateUpdate -> {
          val attributeData =
            subscriptionState.updateState.successes.filterIsInstance<ReadData.Attribute>().firstOrNull {
              it.path.attributeId == ATTRIBUTE_ID
            }        
             
          requireNotNull(attributeData) { 
            "Clusterrevision attribute not found in Node State update" 
          }

          // Decode the TLV data into the appropriate type
          val tlvReader = TlvReader(attributeData.data)
          val decodedValue: UShort = tlvReader.getUShort(AnonymousTag)

          emit(UShortSubscriptionState.Success(decodedValue))
        }
        SubscriptionState.SubscriptionEstablished -> {
          emit(UShortSubscriptionState.SubscriptionEstablished)
        }
      }
    }    
  }

  companion object {
    private val logger = Logger.getLogger(LuminaireDiagnosticsMaintenanceCluster::class.java.name)
    const val CLUSTER_ID: UInt = 772u
  }
}
