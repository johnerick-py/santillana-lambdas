import boto3
import json
import uuid
import time
import random
from datetime import datetime, timezone
import concurrent.futures
import threading

# Configurar o cliente Kinesis usando o perfil nomeado
session = boto3.Session(profile_name="john-prod")
kinesis_client = session.client(
    "kinesis", region_name="us-east-1"
)

# Nome do stream Kinesis
STREAM_NAME = "teste-call-suport"

# Lista de shards e hash keys para distribuição
SHARDS = [
    "shardId-000000000000",
    "shardId-000000000001",
    "shardId-000000000002",
    "shardId-000000000003",
]

# Lista de possíveis ações
ACTIONS = ["user_created", "user_updated", "user_deleted"]

# Template do JSON
BASE_JSON = {
    "meta": {
        "creationDatetime": "2025-01-02T13:09:29.848+00:00",
        "action": "user_updated",  # Será sobrescrito no worker
        "company": "",
        "country": "BR",
        "messageId": "81db0ec4-5a4b-4167-8a33-5e12080c7249",
        "entityKey": "USER",
        "providerName": "SMS_CONSUCORP_PUBL_PRO",
    },
    "data": {
        "eventId": "",  # Será preenchido com UUID no worker
        "user_id": "",  # Será preenchido com UUID no worker
        "attributes": {
            "refId": "00000000-0000-1000-0000-000019256770",
            "country": {"refId": "00000000-0000-1000-0000-000000000044"},
            "sex": "Female",
            "name": {
                "firstName": "JOHN",
                "middleName": "TESTE2",
                "lastName": "TESTEUPDATED2",
            },
            "birthDate": "",
            "language": {"refId": "00000000-0000-1000-0000-000000000000"},
            "addressList": [
                {
                    "address": {
                        "addressType": "Physical",
                        "street": {"line1": "AV NITEROI"},
                        "city": "",
                        "neighborhood": "RETIRO SAO JOAQUIM",
                        "postalCode": "24813-327",
                        "buildingSiteNumber": " S/N",
                        "county": {"refId": "00000000-0000-1000-0000-000000020207"},
                    }
                },
                {
                    "address": {
                        "addressType": "Mailing",
                        "street": {"line1": "AV NITEROI"},
                        "city": "ITABORAÍ",
                        "neighborhood": "RETIRO SAO JOAQUIM",
                        "postalCode": "24813-327",
                        "buildingSiteNumber": " S/N",
                        "county": {"refId": "00000000-0000-1000-0000-000000020207"},
                    }
                },
            ],
            "phoneNumberList": [
                {
                    "phoneNumber": {
                        "phoneNumberType": "Home",
                        "number": "21 99982 6115",
                    }
                }
            ],
            "personEmailList": [{"personEmail": {"email": "john@gmail.com"}}],
            "createdAt": 0,
            "dischargeDate": 0,
            "image": "",
            "oficialId": "",
            "timeZone": {"name": "America/Mexico_City"},
        },
    },
}

# Lock para impressão sincronizada
print_lock = threading.Lock()

import pytz

def get_utc_time():
    """Returns a timestamp atual em UTC no formato ISO 8601."""
    return datetime.now(timezone.utc).isoformat()

def publish_record(record_data, shard_number):
    """
    Publica um registro no Kinesis.
    Usa ExplicitHashKey para forçar o envio para um shard específico.
    """
    try:
        hash_step = 2**128 // 4
        hash_value = shard_number * hash_step
        explicit_hash_key = str(hash_value)

        response = kinesis_client.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(record_data),
            PartitionKey="userarticlenotification",
            ExplicitHashKey=explicit_hash_key,
        )

        return {
            "success": True,
            "shard_id": response["ShardId"],
            "sequence_number": response["SequenceNumber"],
            "record": record_data,
            "target_shard": SHARDS[shard_number],
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "record": record_data,
            "target_shard": SHARDS[shard_number] if shard_number < len(SHARDS) else "unknown",
        }

def worker(worker_id, total_messages, results, start_index):
    """Função de worker para publicar mensagens em um thread separado."""
    for i in range(start_index, start_index + total_messages):
        # Cria uma cópia do JSON base
        record_data = json.loads(json.dumps(BASE_JSON))  # Deep copy

        # Define IDs únicos e timestamp
        record_data["id"] = f"event-{uuid.uuid4()}"
        record_data["datepublish"] = get_utc_time()
        record_data["data"]["eventId"] = str(uuid.uuid4())
        record_data["data"]["user_id"] = str(uuid.uuid4())
        record_data["meta"]["action"] = random.choice(ACTIONS)  # Escolhe ação aleatoriamente

        # Determina qual shard usar (round-robin)
        shard_index = i % len(SHARDS)

        result = publish_record(record_data, shard_index)
        results.append(result)

        if (i - start_index + 1) % 10 == 0:
            with print_lock:
                print(f"Worker {worker_id}: {i - start_index + 1}/{total_messages} mensagens enviadas")

        if result["success"]:
            with print_lock:
                seq_num = result["sequence_number"]
                timestamp = result["record"]["datepublish"]
                print(f"✅ Mensagem publicada - ID: {record_data['id']}")
                print(f"  → Número de sequência: {seq_num}")
                print(f"  → Timestamp (datepublish): {timestamp}")
                print(f"  → Shard: {result['shard_id']}")
                print(f"  → Action: {record_data['meta']['action']}")  # Log da ação

# Restante do código permanece igual
def publish_batch(total_messages, num_workers):
    results = []
    start_time = time.time()

    messages_per_worker = total_messages // num_workers
    remainder = total_messages % num_workers

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        start_index = 0

        for worker_id in range(num_workers):
            worker_messages = messages_per_worker + (1 if worker_id < remainder else 0)
            if worker_messages > 0:
                futures.append(
                    executor.submit(
                        worker, worker_id, worker_messages, results, start_index
                    )
                )
                start_index += worker_messages

        for future in concurrent.futures.as_completed(futures):
            future.result()

    end_time = time.time()
    total_time = end_time - start_time

    success_count = sum(1 for r in results if r["success"])
    failure_count = len(results) - success_count

    print(
        f"""
========== RESULTADOS ==========
Total de mensagens: {total_messages}
Sucesso: {success_count}
Falhas: {failure_count}
Tempo total: {total_time:.2f} segundos
Throughput: {(success_count / total_time):.2f} mensagens/segundo
============================
"""
    )

    return {
        "results": results,
        "summary": {
            "total_messages": total_messages,
            "successful": success_count,
            "failed": failure_count,
            "total_time": total_time,
            "throughput": success_count / total_time,
        },
    }

def generate_report(data):
    shard_stats = {
        shard_id: {"message_count": 0, "success_count": 0, "failure_count": 0}
        for shard_id in SHARDS
    }
    shard_stats["unknown"] = {
        "message_count": 0,
        "success_count": 0,
        "failure_count": 0,
    }

    target_vs_actual = {shard_id: {"target": 0, "actual": 0} for shard_id in SHARDS}

    for result in data["results"]:
        target_shard = result.get("target_shard", "unknown")
        if target_shard in target_vs_actual:
            target_vs_actual[target_shard]["target"] += 1

        if result["success"]:
            shard_id = result["shard_id"]
            shard_stats[shard_id]["message_count"] += 1
            shard_stats[shard_id]["success_count"] += 1
            if shard_id in target_vs_actual:
                target_vs_actual[shard_id]["actual"] += 1
        else:
            shard_stats["unknown"]["message_count"] += 1
            shard_stats["unknown"]["failure_count"] += 1

    print("\n========== ESTATÍSTICAS POR SHARD ==========")
    for shard_id, stats in shard_stats.items():
        if stats["message_count"] > 0:
            success_rate = (stats["success_count"] / stats["message_count"]) * 100
            print(
                f"""
Shard: {shard_id}
  Mensagens: {stats['message_count']}
  Sucesso: {stats['success_count']}
  Falhas: {stats['failure_count']}
  Taxa de Sucesso: {success_rate:.2f}%"""
            )

    print("\n===== DISTRIBUIÇÃO ALVO vs. REAL =====")
    for shard_id, counts in target_vs_actual.items():
        print(f"Shard: {shard_id}")
        print(f"  Mensagens alvo: {counts['target']}")
        print(f"  Mensagens reais: {counts['actual']}")
        if counts["target"] > 0:
            match_rate = (counts["actual"] / counts["target"]) * 100
            print(f"  Taxa de correspondência: {match_rate:.2f}%")

    print("\n=====================================")

def calculate_latency(data):
    if not data["results"]:
        return

    latencies = []

    print("\n========== DETALHES PARA BUSCA NO CONSOLE KINESIS ==========")
    for i, result in enumerate(data["results"]):
        if i >= 5:
            break
        if result["success"]:
            seq_num = result["sequence_number"]
            timestamp = result["record"]["datepublish"]
            print(f"\nMensagem {i+1}:")
            print(f"  • ID: {result['record']['id']}")
            print(f"  • Número de sequência: {seq_num}")
            print(f"  • Horizonte de corte / Timestamp: {timestamp}")
            print(f"  • Shard: {result['shard_id']}")
            print(f"  • Action: {result['record']['meta']['action']}")  # Adiciona action no log
    print("============================================================\n")

    for result in data["results"]:
        if result["success"] and "datepublish" in result["record"]:
            try:
                publish_time = datetime.fromisoformat(
                    result["record"]["datepublish"].rstrip("Z")
                )
                sequence_time = int(result["sequence_number"][:13]) / 1000
                sequence_datetime = datetime.fromtimestamp(sequence_time)
                latency_ms = (sequence_datetime - publish_time).total_seconds() * 1000
                latencies.append(latency_ms)
            except (ValueError, TypeError) as e:
                print(f"Erro ao calcular latência: {e}")

    if latencies:
        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)

        print(
            f"""
========== ESTATÍSTICAS DE LATÊNCIA ==========
Latência média: {avg_latency:.2f} ms
Latência mínima: {min_latency:.2f} ms
Latência máxima: {max_latency:.2f} ms
Amostras: {len(latencies)}
============================================
"""
        )

def run_throughput_test():
    total_messages = 25
    num_workers = 4

    print(
        f"""
========== CONFIGURAÇÃO DO TESTE ==========
Total de mensagens: {total_messages}
Número de workers: {num_workers}
Shards alvo: {', '.join(SHARDS)}
Chave de partição: "teste"
Stream Kinesis: {STREAM_NAME}
Ações possíveis: {', '.join(ACTIONS)}
=======================================
"""
    )

    try:
        data = publish_batch(total_messages, num_workers)
        generate_report(data)
        calculate_latency(data)
        return data
    except Exception as e:
        print(f"Erro ao executar teste de throughput: {e}")
        raise

if __name__ == "__main__":
    print("Iniciando teste de throughput do Kinesis...")
    run_throughput_test()
    print("Teste concluído!")