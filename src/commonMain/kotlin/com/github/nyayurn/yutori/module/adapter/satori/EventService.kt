/*
Copyright (c) 2024 Yurn
Yutori-Next is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
 */

@file:Suppress("MemberVisibilityCanBePrivate", "unused", "HttpUrlsUsage")

package com.github.nyayurn.yutori.module.adapter.satori

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.nyayurn.yutori.*
import com.github.nyayurn.yutori.LoggerColor.blue
import com.github.nyayurn.yutori.LoggerColor.cyan
import com.github.nyayurn.yutori.LoggerColor.gray
import io.ktor.client.*
import io.ktor.client.plugins.websocket.*
import io.ktor.http.*
import io.ktor.serialization.*
import io.ktor.serialization.jackson.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.request.*
import io.ktor.server.routing.*
import io.ktor.websocket.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference


/**
 * Satori 事件服务的 WebSocket 实现
 * @param properties Satori Server 配置
 * @param satori Satori 配置
 */
class WebSocketEventService(
    val properties: SatoriProperties,
    val open: () -> Unit = { },
    val connect: (List<Login>, SatoriActionService, Satori) -> Unit = { _, _, _ -> },
    val error: () -> Unit = { },
    val satori: Satori
) : EventService {
    private val status = AtomicReference<ServiceStatus>(Initializing)
    private val reconnectLock = Mutex()
    private val is_received_pong = AtomicBoolean(false)
    private var sequence: Number? = null
    private val service = SatoriActionService(properties, satori.name)
    private val client = AtomicReference<HttpClient?>(null)
    private val logger = GlobalLoggerFactory.getLogger(this::class.java)

    @OptIn(DelicateCoroutinesApi::class)
    override suspend fun connect() {
        val currentClient = HttpClient {
            install(WebSockets) {
                contentConverter = JacksonWebsocketContentConverter()
            }
        }
        client.set(currentClient)
        val name = satori.name
        GlobalScope.launch {
            try {
                currentClient.webSocket(
                    HttpMethod.Get, properties.host, properties.port, "${properties.path}/${properties.version}/events"
                ) {
                    try {
                        open()
                        sendSerialized(IdentifySignal(Identify(properties.token, sequence)))
                        logger.info(name, "成功建立 WebSocket 连接, 尝试建立事件推送服务")
                        withTimeoutOrNull(10000L) {
                            val ready = receiveDeserialized<ReadySignal>().body
                            connect(ready.logins, service, satori)
                            status.set(Running)
                            logger.info(name, "成功建立事件推送服务: ${ready.logins}")
                        } ?: throw TimeoutException("无法建立事件推送服务: READY 响应超时")
                        sendPing()
                        while (true) try {
                            when (val signal = receiveDeserialized<Signal>()) {
                                is EventSignal -> onEvent(signal.body)
                                is PongSignal -> {
                                    is_received_pong.set(true)
                                    logger.debug(name, "收到 PONG")
                                    sendPing()
                                }
                            }
                        } catch (e: JsonConvertException) {
                            logger.warn(name, "事件解析错误: ${e.localizedMessage}")
                        }
                    } catch (e: Exception) {
                        reconnectLock.withLock {
                            if (currentClient == client.get() && status.get() == Running) {
                                status.set(Reconnecting)
                                error()
                                logger.warn(name, "WebSocket 连接断开: ${e.localizedMessage}")

                                launch {
                                    e.printStackTrace()
                                    close()
                                    logger.info(name, "将在5秒后尝试重新连接")
                                    delay(5000)
                                    logger.info(name, "尝试重新连接")
                                    reconnect()
                                }
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                error()
                logger.warn(name, "WebSocket 连接建立失败: ${e.localizedMessage}")
                e.printStackTrace()
                logger.info(name, "将在5秒后尝试重新连接")
                delay(5000)
                logger.info(name, "尝试重新连接")
                reconnect()
            }
        }
    }

    private fun DefaultClientWebSocketSession.sendPing() = launch {
        delay(9000)
        is_received_pong.set(false)
        sendSerialized(PingSignal)
        val name = satori.name
        logger.debug(name, "发送 PING")
        delay(10000)
        // Mutex保证状态的更新和重连接为一个原子操作，不被途中插入
        // AtomicReference保证多线程可见性
        reconnectLock.withLock {
            if (!is_received_pong.get() && this@sendPing.call.client == client.get() && status.get() == Running) {
                status.set(Reconnecting)
                error()
                logger.warn(name, "WebSocket 连接断开: PONG 响应超时")
                launch {
                    close()
                    logger.info(name, "将在5秒后尝试重新连接")
                    delay(5000)
                    logger.info(name, "尝试重新连接")
                    reconnect()
                }
            }
        }
    }

    private fun DefaultClientWebSocketSession.onEvent(event: Event<AnyEvent>) = launch {
        val name = satori.name
        try {
            when (event.type) {
                MessageEvents.Created -> logger.info(name, buildString {
                    append("${event.platform}(${event.self_id}) 接收事件(${event.type}): ")
                    append(blue("${event.nullable_channel!!.name}(${event.nullable_channel!!.id})"))
                    append(gray("-"))
                    append(cyan("${event.nick()}(${event.nullable_user!!.id})"))
                    append(": ")
                    append(event.nullable_message!!.content)
                })

                else -> logger.info(
                    name, "${event.platform}(${event.self_id}) 接收事件: ${event.type}"
                )
            }
            logger.debug(name, "事件详细信息: $event")
            sequence = event.id
            satori.container(event, satori, service)
        } catch (e: Exception) {
            logger.warn(name, "处理事件时出错($event): ${e.localizedMessage}")
            e.printStackTrace()
        }
    }

    override fun disconnect() {
        client.get()?.close()
        client.set(null)
        status.set(Stopped)
    }

    override suspend fun reconnect() { //TODO: 外部调用多线程问题
        client.get()?.close()
        client.set(null)
        connect()
    }
}

/**
 * Satori 事件服务的 WebHook 实现
 * @param properties Satori WebHook 配置
 * @param satori Satori 配置
 */
class WebhookEventService(
    val listen: String, val port: Int, val path: String, val properties: SatoriProperties, val satori: Satori
) : EventService {
    private var client: ApplicationEngine? = null
    private val service = SatoriActionService(properties, satori.name)
    private val logger = GlobalLoggerFactory.getLogger(this::class.java)

    override suspend fun connect() {
        client = embeddedServer(io.ktor.server.cio.CIO, port, listen) {
            routing {
                post(path) {
                    val authorization = call.request.headers["Authorization"]
                    if (authorization != properties.token) {
                        call.response.status(HttpStatusCode.Unauthorized)
                        return@post
                    }
                    val body = call.receiveText()
                    try {
                        val mapper = jacksonObjectMapper().configure(
                            DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false
                        )
                        val event = mapper.readValue<Event<AnyEvent>>(body)
                        launch {
                            when (event.type) {
                                in MessageEvents.Types -> logger.info(satori.name, buildString {
                                    append("${event.platform}(${event.self_id}) 接收事件(${event.type}): ")
                                    append("\u001B[38;5;4m").append(
                                        "${event.nullable_channel!!.name}(${
                                            event.nullable_channel!!.id
                                        })"
                                    )
                                    append("\u001B[38;5;6m")
                                    append(event.nick())
                                    append("(${event.nullable_user!!.id})")
                                    append("\u001B[0m").append(": ").append(event.nullable_message!!.content)
                                })

                                else -> logger.info(
                                    satori.name, "${event.platform}(${event.self_id}) 接收事件: ${event.type}"
                                )
                            }
                            logger.debug(satori.name, "事件详细信息: $body")
                            satori.container(event, satori, service)
                        }
                        call.response.status(HttpStatusCode.OK)
                    } catch (e: Exception) {
                        logger.warn(satori.name, "处理事件时出错(${body}): ${e.localizedMessage}")
                        e.printStackTrace()
                        call.response.status(HttpStatusCode.InternalServerError)
                    }
                }
            }
        }.start()
        logger.info(satori.name, "成功启动 HTTP 服务器")
        RootActions.AdminAction(service).webhook.create(
            "http://${properties.host}:${properties.port}${properties.path}", properties.token
        )
    }

    override suspend fun reconnect() {
        TODO("Not yet implemented")
    }

    override fun disconnect() {
        RootActions.AdminAction(service).webhook.delete(
            "http://${properties.host}:${properties.port}${properties.path}"
        )
        client?.stop()
    }
}

/**
 * 服务状态
 */
sealed interface ServiceStatus

/**
 * 初始化
 */
object Initializing : ServiceStatus

/**
 * 运行中
 */
object Running : ServiceStatus

/**
 * 重连接
 */
object Reconnecting : ServiceStatus

/**
 * 停止
 */
object Stopped : ServiceStatus