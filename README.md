# 🧠 Federación de Hubs NeuralProtocol - Demo de Compra-Venta-Facturación

Este proyecto demuestra la **federación entre dos hubs** de NeuralProtocol, simulando una interacción B2B entre una empresa de compras (Hub A, dominio `empresa-a.com`) y una empresa de ventas (Hub B, dominio `empresa-b.com`). Se utilizan tres agentes:

- **`comprador`** (hub A) – Solicita productos.
- **`vendedor`** (hub B) – Recibe pedidos, confirma y coordina con facturación.
- **`facturacion`** (hub B) – Genera facturas y las envía al vendedor.

El flujo completo incluye:

1. El comprador envía una solicitud de compra a `vendedor@empresa-b.com` (señal `NOREPINEPHRINE`).
2. El vendedor responde inmediatamente al comprador (señal `DOPAMINE`) y envía una tarea a facturación (señal `GLUTAMATE`).
3. Facturación genera una factura y la envía al vendedor (señal `SEROTONIN`).
4. El vendedor reenvía la factura al comprador (señal `DOPAMINE`).

Todo el tráfico entre hubs utiliza el protocolo de federación implementado (Fase 1), con autenticación mediante token compartido y reenvío de señales a través de conexiones hub‑hub.

---

## 📋 Requisitos previos

- Python 3.9 o superior.
- Los paquetes `neural-protocol` y `neural-hub` instalados (preferiblemente desde los repositorios con las modificaciones para federación).
- Opcional: `aiohttp` para el dashboard (`pip install neural-hub[dashboard]`).

Si aún no tienes los paquetes modificados, clona los repositorios y ejecuta `pip install -e .` en cada uno:

```bash
git clone https://github.com/firecode16/neural-protocol.git
cd neural-protocol
pip install -e .
cd ..
git clone https://github.com/firecode16/neural-hub.git
cd neural-hub
pip install -e .[dashboard]   # si quieres el dashboard
cd ..
```

Asegúrate de que los cambios de federación (parámetro `--domain`, corrección en `send_ctrl`, etc.) estén presentes en tu instalación de `neural-hub`. Puedes copiar los archivos `server.py` y `run_hub.py` proporcionados en la solución anterior.

---

## 📁 Estructura del proyecto

Crea una carpeta, por ejemplo `federacion-demo`, y dentro de ella los siguientes archivos:

```
federacion-demo/
├── hub_a_config.json          # Configuración del hub A (empresa-a.com)
├── hub_b_config.json          # Configuración del hub B (empresa-b.com)
├── agente_compra.py           # Agente comprador (hub A)
├── agente_venta.py            # Agente vendedor (hub B)
├── agente_facturacion.py      # Agente facturación (hub B)
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

**Importante:** El token debe coincidir en ambos archivos. Los dominios (`empresa-a.com`, `empresa-b.com`) son los que se usarán al iniciar los hubs con el parámetro `--domain`.

---

## 🤖 Scripts de los agentes

### `agente_compra.py`

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

### `agente_venta.py`

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

            # 1. Responder al comprador
            respuesta_inmediata = {
                "tipo": "confirmacion_pedido",
                "producto": producto,
                "monto": monto,
                "estado": "recibido"
            }
            await self.transmit(cliente_global, NeuralSignalType.DOPAMINE, respuesta_inmediata)
            print(f"✅ Confirmación enviada a {cliente_global}")

            # 2. Enviar tarea a facturación
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

### `agente_facturacion.py`

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
            await asyncio.sleep(1)  # simular procesamiento
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

---

## 🚀 Ejecución del demo

Sigue estos pasos en orden. Necesitarás **cinco terminales** (o cuatro si no usas dashboard).

### 1. Iniciar el hub B (ventas, puerto 8766)

```bash
python -m neural_hub.scripts.run_hub --port 8766 --domain empresa-b.com --remote-hubs hub_b_config.json --dashboard-port 8081
```

### 2. Iniciar el hub A (compras, puerto 8765)

```bash
python -m neural_hub.scripts.run_hub --port 8765 --domain empresa-a.com --remote-hubs hub_a_config.json --dashboard-port 8080
```

Verás que los hubs se conectan mutuamente (mensaje `🔌 Hub remoto conectado (entrante): ...`).

### 3. Iniciar el agente de facturación (conecta al hub B)

```bash
python agente_facturacion.py
```

### 4. Iniciar el agente vendedor (conecta al hub B)

```bash
python agente_venta.py
```

### 5. Iniciar el agente comprador (conecta al hub A)

```bash
python agente_compra.py
```

Una vez que todos los agentes estén en línea, en la terminal del **comprador** escribe un comando como:

```
enviar vendedor@empresa-b.com tornillos 250
```

---

## 📊 Visualización con los dashboards

- Abre `http://localhost:8080` para ver el dashboard del hub A.
- Abre `http://localhost:8081` para ver el dashboard del hub B.

Ambos dashboards mostrarán en tiempo real los agentes conectados, las sinapsis, el tráfico de señales y las estadísticas.

---

## 🔍 Resultado esperado

Después de enviar el pedido, deberías ver en las terminales:

- **Comprador**: dos respuestas entrantes (confirmación y factura).
- **Vendedor**: recibe el pedido, envía confirmación, envía tarea a facturación, recibe factura y la reenvía.
- **Facturación**: recibe la tarea, genera una factura y responde al vendedor.
- **Hubs**: registran los reenvíos y las señales.

Ejemplo de salida (resumida):

**Comprador:**
```
📤 🟡 NeuralSignal [NOREPINEPHRINE] ... (a 'vendedor@empresa-b.com')
✅ Solicitud enviada a vendedor@empresa-b.com

📥 Respuesta de 9e6dc8c6: {'tipo': 'confirmacion_pedido', ...}
📥 Respuesta de 9e6dc8c6: {'tipo': 'factura', 'factura_id': 'FAC-1797', ...}
```

**Vendedor:**
```
📥 Vendedor recibe: {'producto': 'tornillos', 'monto': 250.0, 'de': 'comprador@empresa-a.com', ...}
✅ Confirmación enviada a comprador@empresa-a.com
✅ Tarea enviada a facturación
📥 Vendedor recibe: {'factura_id': 'FAC-1797', 'cliente': 'comprador@empresa-a.com', ...}
✅ Factura reenviada a comprador@empresa-a.com
```

**Facturación:**
```
📥 Facturación recibe: {'cliente': 'comprador@empresa-a.com', 'producto': 'tornillos', ...}
✅ Factura FAC-1797 generada y enviada a vendedor
```

**Hub A:**
```
📨 Señal reenviada desde empresa-b.com para comprador
  ↪ (reenviado) 🟢 → comprador
```

**Hub B:**
```
📨 Señal reenviada desde empresa-a.com para vendedor
  ↪ (reenviado) 🟡 → vendedor
  ↪ 🟣 vendedor→facturacion (round-robin 'facturacion') (182B)
  ↪ 🔵 facturacion→vendedor (hash) (253B)
```

---

## 🐞 Solución de problemas

| Problema | Posible causa | Solución |
|----------|---------------|----------|
| Los hubs no se conectan entre sí | Token incorrecto o dominio no coincide | Verifica que los archivos JSON usen los mismos dominios y token. Asegúrate de que los hubs se inicien con `--domain` correcto. |
| Error `ctrl_msg() missing 1 required positional argument` | Versión antigua de `neural-hub` sin la corrección en `send_ctrl` | Aplica el parche en `server.py` (extraer `_ctrl` del diccionario). |
| El vendedor no puede responder porque el hash del comprador no es conocido | El comprador no incluyó su nombre completo en el payload | Usa el campo `"de": "comprador@dominio"` en la solicitud inicial, como en el script actualizado. |
| Las señales no llegan al destino remoto | El hub no tiene configurado el dominio remoto | Revisa que el dominio en el destino (`vendedor@empresa-b.com`) coincida con una clave en `remote_hubs`. |

---

## 📚 Notas adicionales

- La federación entre hubs utiliza una conexión WebSocket persistente con reconexión automática.
- Los agentes pueden enviar señales a destinos remotos usando la notación `nombre@dominio`.
- El hub resuelve los nombres locales mediante round‑robin si hay varios agentes con el mismo nombre.
- Las sinapsis se persisten en SQLite y pueden consultarse a través del dashboard.
- Este demo corresponde a la **Fase 1** de federación (conexión básica). Fases posteriores añadirían descubrimiento dinámico, alta disponibilidad, etc.

¡Disfruta explorando la comunicación B2B con NeuralProtocol! Si deseas extender el demo (por ejemplo, añadiendo más agentes o lógica de negocio), los scripts son fácilmente modificables.