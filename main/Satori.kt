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

@file:Suppress("MemberVisibilityCanBePrivate", "unused")

package github.nyayurn.yutori_next

class Satori(val config: Config) {
    fun start() {
        for (module in config.modules) {
            module.install(this)
        }
    }

    fun stop() {
        for (module in config.modules) {
            module.uninstall(this)
        }
    }

    companion object {
        fun of(name: String = "Satori", block: SatoriBuilder.() -> Unit) = SatoriBuilder(name).apply(block).build()
    }

    class SatoriBuilder(var name: String) {
        var container: ListenersContainer = ListenersContainer.of()
        var modules: MutableList<Module> = mutableListOf()

        fun <T : Module> install(func: () -> T, block: T.() -> Unit) {
            modules += func().apply(block)
        }

        fun <T : Module> install(func: () -> T) {
            modules += func()
        }

        fun listening(lambda: ListenersContainer.() -> Unit) {
            container = ListenersContainer.of(lambda)
        }

        fun build() = Satori(Config(name, container, modules))
    }
}

interface Module {
    fun install(satori: Satori)
    fun uninstall(satori: Satori)
}

class Config(val name: String, val container: ListenersContainer, val modules: MutableList<Module>)