#!/usr/bin/env python3
"""
Agente Monitor (Hub B, dominio empresa-b.com)
Se conecta y desconecta periódicamente para probar las actualizaciones dinámicas de peers (HUB_PEER_UPDATE).
"""
import asyncio
import random
from neural_protocol.agent.base_ws import WSNeuralAgent
from neural_protocol.core.signal import NeuralSignalType

class AgenteMonitor(WSNeuralAgent):
    async def handle_signal(self, signal):
        """Ignora las señales (solo queremos probar presencia)."""
        pass  # No hace nada

async def main():
    # Ciclo infinito: conectar, esperar un rato, desconectar, esperar, repetir
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
            tiempo_activo = random.randint(5, 15)  # Entre 5 y 15 segundos
            print(f"🟢 Monitor conectado. Permanece activo {tiempo_activo} segundos.")
            await asyncio.sleep(tiempo_activo)
            await agente.stop()
            print("🔴 Monitor desconectado.")
            
            tiempo_espera = random.randint(5, 10)
            print(f"💤 Esperando {tiempo_espera} segundos antes de reconectar...")
            await asyncio.sleep(tiempo_espera)
        except KeyboardInterrupt:
            print("\n🛑 Monitor detenido por el usuario.")
            break
        except Exception as e:
            print(f"❌ Error en monitor: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Monitor finalizado.")