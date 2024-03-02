# 介绍

- Yutori-Next 是一个 [Satori 协议](https://satori.chat) 的 [Kotlin](https://kotlinlang.org) 多平台机器人开发框架
- Yutori-Next 的名称来源于作者 Yurn 与 Satori 名字的结合
- 本项目不依赖也不建议配合 Spring 进行开发

## 依赖引入

- 获取版本请查看 [Jitpack](https://jitpack.io/#Nyayurn/Yutori-Next)

### Maven

```xml
<repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
</repository>
```

```xml
<dependency>
    <groupId>com.github.Nyayurn</groupId>
    <artifactId>Yutori-Next</artifactId>
    <version>${version}</version>
</dependency>
```

### Gradle Kotlin DSL

```kotlin
maven { url = URI("https://jitpack.io") }
```

```kotlin
implementation("com.github.Nyayurn:Yutori-Next:$version")
```

### Gradle Groovy DSL


```groovy
maven { url 'https://jitpack.io' }
```

```groovy
implementation 'com.github.Nyayurn:Yutori:${version}'
```

## 基础使用

```kotlin
fun main() {
    WebSocketEventService.connect {
        listeners {
            message.created { actions, event ->
                if (event.message.content == "在吗") {
                    actions.message.create(event.channel.id) {
                        at { id = event.user.id }
                        text { " 我在!" }
                    }
                }
            }
        }
        properties {
            token = "token"
        }
    }
}
```