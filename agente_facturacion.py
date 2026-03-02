#!/usr/bin/env python3
import asyncio
import random
from neural_protocol.agent.base_ws import WSNeuralAgent
from neural_protocol.core.signal import NeuralSignalType

class AgenteFacturacion(WSNeuralAgent):
    async def handle_signal(self, signal):
        print(f"\n📥 Facturación recibe: {signal.payload}")

        if signal.signal_type == NeuralSignalType.GLUTAMATE:
            # Simular generación de factura
            await asyncio.sleep(1)  # simular procesamiento
            factura_id = f"FAC-{random.randint(1000,9999)}"
            respuesta = {
                "factura_id": factura_id,
                "cliente": signal.payload.get("cliente"),   # ← nombre completo
                "producto": signal.payload.get("producto"),
                "monto": signal.payload.get("monto"),
                "pedido_id": signal.payload.get("pedido_id"),
                "estado": "emitida",
                "timestamp": asyncio.get_event_loop().time()
            }
            # Responder al vendedor (source original)
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