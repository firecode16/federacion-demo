# 🧠 Federación de Hubs NeuralProtocol - Demo de Compra-Venta-Facturación (Fase 2)

Este proyecto demuestra la **federación entre dos hubs** de NeuralProtocol con **descubrimiento dinámico y presencia (Fase 2)** , simulando una interacción B2B entre una empresa de compras (Hub A, dominio `empresa-a.com`) y una empresa de ventas (Hub B, dominio `empresa-b.com`).

## 🆕 Novedades de la Fase 2

- **Descubrimiento dinámico de agentes remotos**: los hubs intercambian automáticamente las listas de agentes conectados (`HUB_PEER_UPDATE`).
- **Consulta de disponibilidad**: los agentes pueden verificar si un destino remoto está online antes de enviar señales (nuevo método JSON-RPC `remote_agent.discover`).
- **Enrutamiento optimizado**: el hub local solo reenvía señales a hubs remotos si el agente destino existe y está disponible.
- **Heartbeat entre hubs**: conexiones persistentes con ping/pong cada 15 segundos para detectar fallos rápidamente.
- **Agente monitor**: nuevo script que se conecta y desconecta periódicamente para probar las actualizaciones dinámicas.

---

## 📋 Requisitos previos

- Python 3.9 o superior.
- Los paquetes `neural-protocol` y `neural-hub` instalados **con soporte para Fase 2** (versión ≥ 1.2).
- Opcional: `aiohttp` para el dashboard (`pip install neural-hub[dashboard]`).

### Instalación de los paquetes (modo desarrollo)

```bash
git clone https://github.com/firecode16/neural-protocol.git
cd neural-protocol
pip install -e .
cd ..
git clone https://github.com/firecode16/neural-hub.git
cd neural-hub
pip install -e .[dashboard]   # incluye aiohttp para el dashboard
cd ..
```

---

## 📁 Estructura del proyecto

```
federacion-demo/
├── hub_a_config.json          # Configuración del hub A (empresa-a.com)
├── hub_b_config.json          # Configuración del hub B (empresa-b.com)
├── agente_compra.py           # Agente comprador (hub A) - MODIFICADO (consulta disponibilidad)
├── agente_venta.py            # Agente vendedor (hub B)
├── agente_facturacion.py      # Agente facturación (hub B)
├── agente_monitor.py          # NUEVO: agente que se conecta/desconecta para probar descubrimiento
└── README.md                  # Este archivo
```

---

## ⚙️ Configuración de los hubs

### Archivo `hub_a_config.json`
```json
{
    "empresa-b.com": {
        "url": "ws://localhost:8766",
        "token": "secreto123",
        "enabled": true,
        "autoconnect": true
    }
}
```

### Archivo `hub_b_config.json`
```json
{
    "empresa-a.com": {
        "url": "ws://localhost:8765",
        "token": "secreto123",
        "enabled": true,
        "autoconnect": true
    }
}
```

**Importante:** El token debe coincidir en ambos archivos. Los dominios (`empresa-a.com`, `empresa-b.com`) se usan al iniciar los hubs con `--domain`.

---

## 🤖 Scripts de los agentes

### `agente_compra.py` (con verificación de disponibilidad)

```python
#!/usr/bin/env python3
import asyncio
import sys
from neural_protocol.agent.base_ws import WSNeuralAgent
from neural_protocol.core.signal import NeuralSignalType

class AgenteCompra(WSNeuralAgent):
    async def handle_signal(self, signal):
        print(f"\n📥 Respuesta de {signal.source[:8]}: {signal.payload}")

async def main():
    agente = AgenteCompra(
        agent_id="comprador",
        hub_host="localhost",
        hub_port=8765,
        domain="empresa-a.com"
    )
    await agente.start()
    print("🟢 Agente de compras conectado al hub A (empresa-a.com)")
    print("Comandos:")
    print("  enviar <destino@dominio> <producto> <monto>")
    print("  salir")
    print("Ejemplo: enviar vendedor@empresa-b.com tornillos 250")
    print("\n🔍 NUEVO: Antes de enviar, se consulta disponibilidad remota (remote_agent.discover)")

    try:
        while True:
            line = await asyncio.get_event_loop().run_in_executor(None, sys.stdin.readline)
            line = line.strip()
            if not line:
                continue
            if line == "salir":
                break
            if line.startswith("enviar "):
                parts = line.split(maxsplit=3)
                if len(parts) < 4:
                    print("Uso: enviar <destino@dominio> <producto> <monto>")
                    continue
                _, destino, producto, monto = parts
                try:
                    monto = float(monto)
                except ValueError:
                    print("❌ El monto debe ser un número")
                    continue

                # Consultar disponibilidad remota (Fase 2)
                try:
                    print(f"🔍 Consultando disponibilidad de {destino}...")
                    info = await agente.jsonrpc_call(
                        "remote_agent.discover",
                        {"name": destino},
                        timeout=5.0
                    )
                    if not info.get("online"):
                        print(f"⚠️  El agente {destino} no está disponible en este momento.")
                        continue
                    else:
                        print(f"✅ {destino} está disponible. Procediendo con el envío.")
                except Exception as e:
                    print(f"❌ Error al consultar disponibilidad: {e}")
                    continue

                payload = {
                    "producto": producto,
                    "monto": monto,
                    "de": f"comprador@{agente.domain}",
                    "timestamp": asyncio.get_event_loop().time()
                }
                success = await agente.transmit(destino, NeuralSignalType.NOREPINEPHRINE, payload)
                if success:
                    print(f"✅ Solicitud enviada a {destino}")
                else:
                    print("❌ Error al enviar")
            else:
                print("Comando no reconocido")
    finally:
        await agente.stop()
        print("🔴 Agente de compras desconectado")

if __name__ == "__main__":
    asyncio.run(main())
```

### `agente_venta.py` (sin cambios)

```python
#!/usr/bin/env python3
import asyncio
from neural_protocol.agent.base_ws import WSNeuralAgent
from neural_protocol.core.signal import NeuralSignalType

class AgenteVenta(WSNeuralAgent):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pedidos_pendientes = {}

    async def handle_signal(self, signal):
        print(f"\n📥 Vendedor recibe: {signal.payload}")

        if signal.signal_type == NeuralSignalType.NOREPINEPHRINE:
            cliente_global = signal.payload.get("de")
            producto = signal.payload.get("producto", "desconocido")
            monto = signal.payload.get("monto", 0)
            msg_id = signal.msg_id

            if not cliente_global:
                print("⚠️  El payload no incluye 'de', no se puede responder")
                return

            self.pedidos_pendientes[msg_id] = (cliente_global, producto, monto)

            respuesta_inmediata = {
                "tipo": "confirmacion_pedido",
                "producto": producto,
                "monto": monto,
                "estado": "recibido"
            }
            await self.transmit(cliente_global, NeuralSignalType.DOPAMINE, respuesta_inmediata)
            print(f"✅ Confirmación enviada a {cliente_global}")

            factura_payload = {
                "cliente": cliente_global,
                "producto": producto,
                "monto": monto,
                "pedido_id": msg_id
            }
            await self.transmit("facturacion", NeuralSignalType.GLUTAMATE, factura_payload)
            print("✅ Tarea enviada a facturación")

        elif signal.signal_type == NeuralSignalType.SEROTONIN:
            factura = signal.payload
            pedido_id = factura.get("pedido_id")
            if pedido_id in self.pedidos_pendientes:
                cliente_global, producto, monto = self.pedidos_pendientes.pop(pedido_id)
                factura_para_cliente = {
                    "tipo": "factura",
                    "factura_id": factura.get("factura_id"),
                    "producto": producto,
                    "monto": monto,
                    "estado": "emitida"
                }
                await self.transmit(cliente_global, NeuralSignalType.DOPAMINE, factura_para_cliente)
                print(f"✅ Factura reenviada a {cliente_global}")
            else:
                print("⚠️  Factura para pedido desconocido, ignorando")

async def main():
    agente = AgenteVenta(
        agent_id="vendedor",
        hub_host="localhost",
        hub_port=8766,
        domain="empresa-b.com"
    )
    await agente.start()
    print("🟢 Agente de ventas conectado al hub B (empresa-b.com)")
    print("Esperando pedidos... (Ctrl+C para salir)")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        await agente.stop()
        print("🔴 Agente de ventas desconectado")

if __name__ == "__main__":
    asyncio.run(main())
```

### `agente_facturacion.py` (sin cambios)

```python
#!/usr/bin/env python3
import asyncio
import random
from neural_protocol.agent.base_ws import WSNeuralAgent
from neural_protocol.core.signal import NeuralSignalType

class AgenteFacturacion(WSNeuralAgent):
    async def handle_signal(self, signal):
        print(f"\n📥 Facturación recibe: {signal.payload}")

        if signal.signal_type == NeuralSignalType.GLUTAMATE:
            await asyncio.sleep(1)
            factura_id = f"FAC-{random.randint(1000,9999)}"
            respuesta = {
                "factura_id": factura_id,
                "cliente": signal.payload.get("cliente"),
                "producto": signal.payload.get("producto"),
                "monto": signal.payload.get("monto"),
                "pedido_id": signal.payload.get("pedido_id"),
                "estado": "emitida",
                "timestamp": asyncio.get_event_loop().time()
            }
            await self.transmit(signal.source, NeuralSignalType.SEROTONIN, respuesta)
            print(f"✅ Factura {factura_id} generada y enviada a vendedor")

async def main():
    agente = AgenteFacturacion(
        agent_id="facturacion",
        hub_host="localhost",
        hub_port=8766,
        domain="empresa-b.com"
    )
    await agente.start()
    print("🟢 Agente de facturación conectado al hub B")
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        await agente.stop()
        print("🔴 Agente de facturación desconectado")

if __name__ == "__main__":
    asyncio.run(main())
```

### 🆕 `agente_monitor.py` (nuevo)

```python
#!/usr/bin/env python3
"""
Agente Monitor (Hub B, dominio empresa-b.com)
Se conecta y desconecta periódicamente para probar las actualizaciones dinámicas de peers.
"""
import asyncio
import random
from neural_protocol.agent.base_ws import WSNeuralAgent

class AgenteMonitor(WSNeuralAgent):
    async def handle_signal(self, signal):
        pass  # No hace nada, solo para probar presencia

async def main():
    while True:
        try:
            agente = AgenteMonitor(
                agent_id="monitor",
                hub_host="localhost",
                hub_port=8766,
                domain="empresa-b.com"
            )
            print("\n🟡 Monitor conectándose...")
            await agente.start()
            tiempo_activo = random.randint(5, 15)
            print(f"🟢 Monitor conectado. Activo {tiempo_activo} segundos.")
            await asyncio.sleep(tiempo_activo)
            await agente.stop()
            print("🔴 Monitor desconectado.")
            await asyncio.sleep(random.randint(5, 10))
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Monitor finalizado.")
```

---

## 🚀 Ejecución del demo (Fase 2)

Necesitarás **seis terminales** para probar todas las capacidades.

### 1. Iniciar los hubs (dos terminales)

**Hub B (ventas, puerto 8766)**
```bash
cd /ruta/a/federacion-demo
python -m neural_hub.scripts.run_hub --port 8766 --domain empresa-b.com --remote-hubs hub_b_config.json --dashboard-port 8081
```

**Hub A (compras, puerto 8765)**
```bash
cd /ruta/a/federacion-demo
python -m neural_hub.scripts.run_hub --port 8765 --domain empresa-a.com --remote-hubs hub_a_config.json --dashboard-port 8080
```

### 2. Iniciar los agentes del hub B (tres terminales)

**Facturación**
```bash
python agente_facturacion.py
```

**Vendedor (puedes ejecutar varias instancias para probar round-robin)**
```bash
python agente_venta.py
```

**Monitor**
```bash
python agente_monitor.py
```

### 3. Iniciar el agente comprador (hub A)

```bash
python agente_compra.py
```

### 4. Probar los escenarios

Desde el comprador, prueba:

| Comando | Comportamiento esperado |
|---------|-------------------------|
| `enviar vendedor@empresa-b.com tornillos 250` | Consulta disponibilidad → online → envía → recibe confirmación y factura |
| `enviar monitor@empresa-b.com prueba 100` (cuando el monitor esté activo) | Consulta → online → envía (aunque el monitor no responde) |
| `enviar monitor@empresa-b.com prueba 100` (cuando el monitor esté desconectado) | Consulta → offline → no envía |
| `enviar falso@empresa-b.com prueba 100` | Consulta → no existe → no envía |

---

## 📊 Visualización con dashboards

- Hub A: [http://localhost:8080](http://localhost:8080)
- Hub B: [http://localhost:8081](http://localhost:8081)

Los dashboards muestran:
- Agentes conectados en tiempo real.
- Número de agentes remotos descubiertos.
- Tráfico de señales y sinapsis.
- Heartbeats y estado de conexiones entre hubs.

---

## 🔍 Resultado esperado (con Fase 2)

### Terminal del comprador
```
enviar vendedor@empresa-b.com tornillos 250
🔍 Consultando disponibilidad de vendedor@empresa-b.com...
✅ vendedor@empresa-b.com está disponible. Procediendo con el envío.
📤 🟡 NeuralSignal [NOREPINEPHRINE] ... (a 'vendedor@empresa-b.com')
✅ Solicitud enviada a vendedor@empresa-b.com

📥 Respuesta de 9d6701bb: {'tipo': 'confirmacion_pedido', ...}
📥 Respuesta de 9d6701bb: {'tipo': 'factura', 'factura_id': 'FAC-1735', ...}

enviar monitor@empresa-b.com prueba 100
🔍 Consultando disponibilidad de monitor@empresa-b.com...
⚠️  El agente monitor@empresa-b.com no está disponible en este momento.
```

### Terminales de los hubs
**Hub A:**
```
[22:11:31] HUB | 📨 Señal reenviada desde empresa-b.com para comprador
[22:11:31] HUB |   ↪ (reenviado) 🟢 → comprador
[22:11:36] HUB | 💓 Heartbeat | agentes=1 señales=1 remotos=2
```

**Hub B:**
```
[22:11:31] HUB | 📨 Señal reenviada desde empresa-a.com para vendedor
[22:11:31] HUB |   ↪ (reenviado) 🟡 → vendedor
[22:11:31] HUB |   ↪ 🟣 vendedor→facturacion (182B)
[22:11:32] HUB |   ↪ 🔵 facturacion→vendedor (252B)
[22:11:36] HUB | 📥 Recibida actualización de peers desde empresa-a.com (1 agentes)
```

### Monitor (en su terminal)
```
🟡 Monitor conectándose...
🟢 Monitor conectado. Activo 10 segundos.
🔴 Monitor desconectado.
💤 Esperando 7 segundos antes de reconectar...
```

---

## 🧪 Validación de la Fase 2

| Funcionalidad | Evidencia |
|--------------|-----------|
| **Heartbeat entre hubs** | Logs periódicos en hubs, reconexión automática |
| **Intercambio dinámico de peers** | `📥 Recibida actualización de peers` cuando monitor se conecta/desconecta |
| **Consulta de disponibilidad** | Comprador usa `remote_agent.discover` antes de enviar |
| **Enrutamiento optimizado** | No se reenvían señales a `monitor` cuando está offline ni a `falso@...` |
| **Resiliencia** | Los hubs actualizan sus registros remotos en tiempo real |

---

## 🐞 Solución de problemas

| Problema | Solución |
|----------|----------|
| `FileNotFoundError: hub_b_config.json` | Ejecuta los hubs desde la carpeta `federacion-demo` |
| Los hubs no se conectan | Verifica tokens y dominios en los JSON |
| No aparecen agentes remotos | Espera a que se complete el primer intercambio de peers (hasta 30s) |
| Error en `jsonrpc_call` | Asegura que `neural-hub` y `neural-protocol` están actualizados a Fase 2 |

---

## 📚 Notas adicionales

- Este demo ahora corresponde a la **Fase 2** de federación (descubrimiento dinámico y presencia).
- Las sinapsis se persisten en SQLite y son visibles en los dashboards.
- Puedes ejecutar múltiples instancias de `vendedor` para probar round-robin.
- La Fase 3 (alta disponibilidad y clustering) permitirá múltiples hubs por dominio y sincronización de estado.

---

## 🎯 Conclusión

Con esta demo validamos que la **Fase 2 del NeuralProtocol** funciona en un escenario realista B2B:
- ✅ Hubs que descubren dinámicamente agentes remotos.
- ✅ Agentes que consultan disponibilidad antes de enviar.
- ✅ Enrutamiento inteligente que ahorra ancho de banda.
- ✅ Heartbeats que garantizan detección rápida de fallos.

¡Listo para escalar a la Fase 3! 🚀

---

**Inspired by the brain – built for AI agents.**  
[NeuralProtocol](https://github.com/firecode16/neural-protocol) | [NeuralHub](https://github.com/firecode16/neural-hub)
