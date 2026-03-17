#!/usr/bin/env python3
import asyncio
from neural_protocol.agent.base_ws import WSNeuralAgent
from neural_protocol.core.signal import NeuralSignalType

class AgenteVenta(WSNeuralAgent):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pedidos_pendientes = {}  # msg_id -> (cliente_global, producto, monto)

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