# resolver_txhashes_snapshot.py

import json
import time
import csv
from datetime import datetime
from web3 import Web3

# ==== CONFIGURACIÓN ====
SNAPSHOT_FILE = "snapshot_mempool_bloque_23748339.json"
RPC_HTTP = "https://eth-mainnet.g.alchemy.com/v2/Mb0w1SreNP0tXz9xGTK9f"
web3 = Web3(Web3.HTTPProvider(RPC_HTTP))
web3_alchemy = Web3(Web3.HTTPProvider("https://eth-mainnet.g.alchemy.com/v2/Mb0w1SreNP0tXz9xGTK9f"))
web3_infura = Web3(Web3.HTTPProvider("https://mainnet.infura.io/v3/9c61effdaa5c4af995478f715ccdebc8"))
web3_quicknode = Web3(Web3.HTTPProvider("https://cool-convincing-wind.quiknode.pro/6f7c19e08d10e8d804cd7ed1b5347a2f6f235534/"))


# ==== FUNCIONES ====
def load_snapshot(file_path):
    with open(file_path, "r") as f:
        return json.load(f)

def collect_unique_hashes(snapshot):
    tx_seen_by = {}
    for source, hashes in snapshot["transactions"].items():
        for h in hashes:
            if h not in tx_seen_by:
                tx_seen_by[h] = set()
            tx_seen_by[h].add(source)
    return tx_seen_by

def get_tx_details_cascada(tx_hash):
    for w3, label in [(web3_alchemy, 'Alchemy'), (web3_infura, 'Infura'), (web3_quicknode, 'QuickNode')]:
        try:
            tx = w3.eth.get_transaction(tx_hash)
            if tx and tx.hash:
                return tx
        except Exception:
            continue
    return None


def export_to_csv(transactions, tx_seen_by, snapshot_ts_ms, output_file):
    with open(output_file, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp_ms","hash","chain_id","from","to","value","nonce",
            "gas","gas_price","gas_tip_cap","gas_fee_cap","data_size",
            "data_4bytes","sources","included_at_block_height",
            "included_block_timestamp_ms","inclusion_delay_ms","tx_type"
        ])

        for tx in transactions:
            try:
                if tx.input and isinstance(tx.input, str) and tx.input.startswith("0x"):
                    data_bytes = bytes.fromhex(tx.input[2:])
                else:
                    data_bytes = b""
            except Exception:
                data_bytes = b""

            data_size = len(data_bytes)
            data_4bytes = data_bytes[:4].hex() if data_size >= 4 else ""

            writer.writerow([
                snapshot_ts_ms,
                tx.hash.hex(),
                tx.get("chainId", ""),
                tx.get("from", ""),
                tx.get("to", ""),
                tx.get("value", 0),
                tx.get("nonce", ""),
                tx.get("gas", 0),
                tx.get("gasPrice", ""),
                tx.get("maxPriorityFeePerGas", ""),
                tx.get("maxFeePerGas", ""),
                len(tx.get("input", "")) // 2,
                tx.get("input", "")[:10],
                ",".join(tx_seen_by.get(tx.hash.hex(), [])),
                "",  # included_at_block_height
                "",  # included_block_timestamp_ms
                "",  # inclusion_delay_ms
                tx.get("type", "")
            ])


# ==== EJECUCIÓN PRINCIPAL ====
def main():
    snapshot = load_snapshot(SNAPSHOT_FILE)
    snapshot_ts = snapshot["timestamp"]
    snapshot_ts_ms = int(datetime.fromisoformat(snapshot_ts).timestamp() * 1000)

    tx_seen_by = collect_unique_hashes(snapshot)
    print(f"Total de hashes únicos en snapshot: {len(tx_seen_by)}")

    resolved = []
    for i, tx_hash in enumerate(tx_seen_by):
        try:
            tx = get_tx_details_cascada(tx_hash)
            if tx and hasattr(tx, "hash"):
                resolved.append(tx)
            if i % 20 == 0:
                print(f"Resueltos {i+1}/{len(tx_seen_by)}")
        except Exception as e:
            print(f"Error al resolver {tx_hash}: {e}")
            continue

    print(f"Total de transacciones resueltas exitosamente: {len(resolved)}")
    output_file = SNAPSHOT_FILE.replace("snapshot_mempool", "mempool_datos").replace(".json", ".csv")
    export_to_csv(resolved, tx_seen_by, snapshot_ts_ms, output_file)
    print(f"\nArchivo CSV guardado en {output_file} con {len(resolved)} transacciones resueltas.")

if __name__ == "__main__":
    main()
