# mempool_capture_multiapi.py

import asyncio
import websockets
import json
import time
from datetime import datetime
from web3 import Web3

# ==== CONFIGURACIÓN DE ENDPOINTS (reemplazá con tus claves reales si hace falta) ====
ALCHEMY_WSS = "wss://eth-mainnet.g.alchemy.com/v2/Mb0w1SreNP0tXz9xGTK9f"
INFURA_WSS = "wss://mainnet.infura.io/ws/v3/9c61effdaa5c4af995478f715ccdebc8"
QUICKNODE_WSS = "wss://cool-convincing-wind.quiknode.pro/6f7c19e08d10e8d804cd7ed1b5347a2f6f235534/"

# RPC para obtener el bloque real (puede ser Alchemy o Infura)
RPC_HTTP = "https://eth-mainnet.g.alchemy.com/v2/Mb0w1SreNP0tXz9xGTK9f"
web3 = Web3(Web3.HTTPProvider(RPC_HTTP))

# ==== ESTRUCTURA DE RESULTADO ====
def create_empty_snapshot():
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "duration_sec": 12,
        "transactions": {
            "alchemy": set(),
            "infura": set(),
            "quicknode": set()
        }
    }

SUBSCRIBE_MSG = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "eth_subscribe",
    "params": ["newPendingTransactions"]
}

# ==== ESCUCHA DE WEBSOCKETS ====
async def listen(provider_name, url, snapshot, start_time):
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps(SUBSCRIBE_MSG))
        await ws.recv()  # confirmación

        while time.time() - start_time < snapshot["duration_sec"]:
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=10)
                data = json.loads(message)
                tx_hash = data.get("params", {}).get("result")
                if tx_hash:
                    snapshot["transactions"][provider_name].add(tx_hash)
            except asyncio.TimeoutError:
                continue

# ==== ESPERAR NUEVO BLOQUE ====
def wait_for_new_block(latest_block):
    print(f"Esperando bloque posterior a #{latest_block}...")
    while True:
        current = web3.eth.block_number
        if current > latest_block:
            print(f"Nuevo bloque detectado: #{current}")
            return current
        time.sleep(1)

# ==== OBTENER BLOQUE POR NÚMERO ====
def get_block_data(block_number):
    blk = web3.eth.get_block(block_number, full_transactions=True)
    return dict(blk)

# ==== FUNCIÓN PRINCIPAL ====
async def main():
    latest_block = web3.eth.block_number
    print(f"Bloque actual: #{latest_block}")

    # 1. Esperar nuevo bloque
    current_block = wait_for_new_block(latest_block)
    reference_time = time.time()

    # 2. Captura de mempool
    snapshot = create_empty_snapshot()
    print("Iniciando captura de mempool por 12 segundos...")
    await asyncio.gather(
        listen("alchemy", ALCHEMY_WSS, snapshot, reference_time),
        listen("infura", INFURA_WSS, snapshot, reference_time),
        listen("quicknode", QUICKNODE_WSS, snapshot, reference_time)
    )

    for k in snapshot["transactions"]:
        snapshot["transactions"][k] = list(snapshot["transactions"][k])

    snapshot_fname = f"snapshot_mempool_bloque_{current_block}.json"
    with open(snapshot_fname, "w") as f:
        json.dump(snapshot, f, indent=2)

    print(f"\nSnapshot guardado en {snapshot_fname} con un total de hashes:")
    for source, txs in snapshot["transactions"].items():
        print(f"- {source}: {len(txs)} hashes")

    # 3. Esperar siguiente bloque
    next_block = wait_for_new_block(current_block)
    blk = get_block_data(next_block)
    blk_fname = f"bloque_{next_block}.json"
    with open(blk_fname, "w") as f:
        json.dump(blk, f, indent=2, default=str)
    print(f"\nBloque guardado en {blk_fname}")
    print(f"Transacciones en el bloque real #{next_block}: {len(blk['transactions'])}")

# ==== EJECUCIÓN ====
if __name__ == "__main__":
    asyncio.run(main())
