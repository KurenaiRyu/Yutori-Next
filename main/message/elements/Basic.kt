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

@file:Suppress("MemberVisibilityCanBePrivate", "ConvertSecondaryConstructorToPrimary")

package com.github.nyayurn.yutori.next.message.elements

import com.github.nyayurn.yutori.next.MessageUtil.encode

/**
 * 消息元素
 */
fun interface MessageElement {
    override fun toString(): String
}

/**
 * 节点消息元素
 * @property nodeName 节点名称
 * @property properties 属性
 * @property children 子元素
 */
abstract class NodeMessageElement(val nodeName: String) : MessageElement {
    val properties: MutableMap<String, Any?> = mutableMapOf()
    val children: MutableList<MessageElement> = mutableListOf()

    /**
     * 获取属性
     * @param key 属性名
     * @return 属性值
     */
    operator fun get(key: String) = properties[key]

    /**
     * 获取子元素
     * @param index 子元素索引
     * @return 消息元素
     */
    operator fun get(index: Int) = children[index]

    /**
     * 设置属性
     * @param key 属性名
     * @param value 属性值
     */
    operator fun set(key: String, value: Any) {
        properties[key] = value
    }

    /**
     * 设置子元素
     * @param index 索引
     * @param value 消息元素
     */
    operator fun set(index: Int, value: MessageElement) {
        children[index] = value
    }

    /**
     * 设置子元素
     * @param index 索引
     * @param text 文本
     */
    operator fun set(index: Int, text: String) {
        children[index] = Text(text)
    }

    /**
     * 添加子元素
     * @param element 消息元素
     */
    operator fun plusAssign(element: MessageElement) {
        children.add(element)
    }

    /**
     * 添加子元素
     * @param text 消息元素
     */
    operator fun plusAssign(text: String) {
        children.add(Text(text))
    }

    /**
     * 添加子元素
     * @param element 消息元素
     */
    fun add(element: MessageElement): NodeMessageElement {
        children.add(element)
        return this
    }

    /**
     * 添加子元素
     * @param text 消息元素
     */
    fun add(text: String): NodeMessageElement {
        children.add(Text(text))
        return this
    }

    override fun toString() = buildString {
        append("<$nodeName")
        for (item in properties) {
            val key = item.key
            val value = item.value ?: continue
            append(" ")
            append(
                when (value) {
                    is String -> "${key}=\"${value.encode()}\""
                    is Number -> "${key}=${value}"
                    is Boolean -> if (value) key else ""
                    else -> throw Exception("Invalid type")
                }
            )
        }
        if (children.isEmpty()) {
            append("/>")
        } else {
            append(">")
            for (item in children) append(item)
            append("</$nodeName>")
        }
    }
}

/**
 * 纯文本
 * @property text 文本
 */
class Text(var text: String) : MessageElement {
    override fun toString() = text.encode()
}

/**
 * 自定义
 * @property content 内容
 */
class Custom(var content: String) : MessageElement {
    override fun toString() = content
}

/**
 * 提及用户
 * @property id 目标用户的 ID
 * @property name 目标用户的名称
 * @property role 目标角色
 * @property type 特殊操作，例如 all 表示 @全体成员，here 表示 @在线成员
 */
class At : NodeMessageElement {
    var id: String? by super.properties
    var name: String? by super.properties
    var role: String? by super.properties
    var type: String? by super.properties

    constructor(
        id: String? = null,
        name: String? = null,
        role: String? = null,
        type: String? = null
    ) : super("at") {
        this.id = id
        this.name = name
        this.role = role
        this.type = type
    }
}

/**
 * 提及频道
 * @property id 目标频道的 ID
 * @property name 目标频道的名称
 */
class Sharp : NodeMessageElement {
    var id: String by super.properties
    var name: String? by super.properties

    constructor(id: String, name: String? = null) : super("sharp") {
        this.id = id
        this.name = name
    }
}

/**
 * 链接
 * @property href 链接的 URL
 */
class Href : NodeMessageElement {
    var href: String by super.properties

    constructor(href: String) : super("a") {
        this.href = href
    }
}