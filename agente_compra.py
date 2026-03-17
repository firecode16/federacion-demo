#!/usr/bin/env python3
"""
Agente Comprador (Hub A, dominio empresa-a.com)
Ahora con verificación de disponibilidad remota antes de enviar (Fase 2).
"""
import asyncio
import sys
from neural_protocol.agent.base_ws import WSNeuralAgent
from neural_protocol.core.signal import NeuralSignalType

class AgenteCompra(WSNeuralAgent):
    async def handle_signal(self, signal):
        """Maneja las respuestas del vendedor."""
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

                # --- Consultar disponibilidad remota (Fase 2) ---
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
                    # Opcional: podríamos continuar igual, pero decidimos no enviar
                    continue
                # -------------------------------------------------------

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