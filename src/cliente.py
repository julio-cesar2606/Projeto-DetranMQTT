import argparse
import json
import os
import threading
import time
import uuid
from datetime import date
import paho.mqtt.client as mqtt

BROKER = os.getenv("MQTT_HOST", "broker")
PORT = int(os.getenv("MQTT_PORT", "1883"))
REPLY_TOPIC = f"detran/replies/client-{uuid.uuid4().hex[:8]}"

pending = {}
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id=f"client-{uuid.uuid4().hex[:6]}")


def on_connect(c, userdata, flags, reason_code, properties):
    c.subscribe(REPLY_TOPIC, qos=1)

def on_message(c, userdata, msg):
    resp = json.loads(msg.payload)
    slot = pending.get(resp.get("corr_id"))
    if slot:
        slot["resp"] = resp
        slot["event"].set()

def conectar():
    client.on_connect, client.on_message = on_connect, on_message
    while True:
        try:
            client.connect(BROKER, PORT, keepalive=60)
            break
        except OSError:
            print("Aguardando broker...")
            time.sleep(2)
    client.loop_start()
    time.sleep(0.5)  # tempo para o tópico de resposta


def call(topic, data, timeout=10):
    corr_id = uuid.uuid4().hex
    slot = {"event": threading.Event(), "resp": None}
    pending[corr_id] = slot
    client.publish(topic, json.dumps({"corr_id": corr_id, "reply_to": REPLY_TOPIC,"data": data}), qos=1)
    ok = slot["event"].wait(timeout)
    pending.pop(corr_id, None)
    if not ok:
        return {"ok": False, "error": "timeout (o microsserviço está no ar?)"}
    return slot["resp"]


def mostrar(resp):
    if resp["ok"]:
        print(json.dumps(resp["data"], indent=2, ensure_ascii=False))
    else:
        print(f"ERRO: {resp['error']}")


MENU = [
    ("Cadastrar condutor", "detran/condutores/cadastrar",
     [("cpf", "CPF", str), ("nome", "Nome", str)]),
    ("Emplacar veículo", "detran/veiculos/emplacar",
     [("placa", "Placa", str), ("modelo", "Modelo", str), ("valor", "Valor", float),
      ("cpf", "CPF do condutor", str), ("ano", "Ano (opcional, enter = atual)", int)]),
    ("Calcular IPVA (2%)", "detran/veiculos/ipva", [("placa", "Placa", str)]),
    ("Transferir proprietário", "detran/veiculos/transferir",
     [("placa", "Placa", str), ("cpf", "CPF do novo dono", str)]),
    ("Lançar multa", "detran/multas/lancar",
     [("ano", "Ano", int), ("descricao", "Descrição", str),
      ("pontuacao", "Pontuação", int), ("placa", "Placa", str)]),
    ("Veículos emplacados em um ano", "detran/veiculos/listar_por_ano",
     [("ano", "Ano", int)]),
    ("Multas de um veículo (com dados do condutor)", "detran/multas/por_veiculo",
     [("placa", "Placa", str), ("ano", "Ano (opcional)", int)]),
    ("Multas de um condutor em um ano", "detran/multas/por_condutor",
     [("cpf", "CPF", str), ("ano", "Ano", int)]),
    ("Multas lançadas em um ano", "detran/multas/por_ano", [("ano", "Ano", int)]),
    ("Top 5 condutores por pontuação", "detran/multas/ranking", []),
]


def menu():
    while True:
        print("\n=== DENATRAN ===")
        for i, (nome, _, _) in enumerate(MENU, 1):
            print(f"{i:2}. {nome}")
        print(" 0. Sair")
        op = input("> ").strip()
        if op == "0":
            return
        if not op.isdigit() or not 1 <= int(op) <= len(MENU):
            print("Opção inválida.")
            continue
        _, topic, campos = MENU[int(op) - 1]
        data = {}
        try:
            for campo, rotulo, cast in campos:
                bruto = input(f"{rotulo}: ").strip()
                if bruto == "" and "opcional" in rotulo:
                    continue
                data[campo] = cast(bruto)
        except ValueError:
            print("Valor inválido.")
            continue
        mostrar(call(topic, data))


def demo():
    ano = date.today().year
    passos = [
        ("Cadastrar condutores", "detran/condutores/cadastrar", {"cpf": "111", "nome": "Ana"}),
        (None, "detran/condutores/cadastrar", {"cpf": "222", "nome": "Bruno"}),
        (None, "detran/condutores/cadastrar", {"cpf": "333", "nome": "Carla"}),
        (None, "detran/condutores/cadastrar", {"cpf": "444", "nome": "Diego"}),
        (None, "detran/condutores/cadastrar", {"cpf": "555", "nome": "Elisa"}),
        (None, "detran/condutores/cadastrar", {"cpf": "666", "nome": "Fábio"}),
        (None, "detran/condutores/cadastrar", {"cpf": "777", "nome": "Gabi"}),
        ("Emplacar veículos", "detran/veiculos/emplacar",
         {"placa": "ABC1D23", "modelo": "Gol", "valor": 50000, "cpf": "111"}),
        (None, "detran/veiculos/emplacar",
         {"placa": "XYZ9K88", "modelo": "Civic", "valor": 120000, "cpf": "222", "ano": ano - 1}),
        (None, "detran/veiculos/emplacar",
         {"placa": "DEF4G56", "modelo": "Onix", "valor": 70000, "cpf": "444"}),
        (None, "detran/veiculos/emplacar",
         {"placa": "GHI7J89", "modelo": "HB20", "valor": 60000, "cpf": "555"}),
        (None, "detran/veiculos/emplacar",
         {"placa": "JKL0M12", "modelo": "Corolla", "valor": 130000, "cpf": "666"}),
        (None, "detran/veiculos/emplacar",
         {"placa": "MNO3P45", "modelo": "Mobi", "valor": 40000, "cpf": "777"}),
        (None, "detran/veiculos/emplacar",
         {"placa": "QWE1R23", "modelo": "Uno", "valor": 1000, "cpf": "999"}),
        ("IPVA de ABC1D23", "detran/veiculos/ipva", {"placa": "ABC1D23"}),
        ("Lançar multas", "detran/multas/lancar",
         {"ano": ano, "descricao": "Excesso de velocidade", "pontuacao": 5, "placa": "ABC1D23"}),
        (None, "detran/multas/lancar",
         {"ano": ano, "descricao": "Farol apagado", "pontuacao": 3, "placa": "ABC1D23"}),
        (None, "detran/multas/lancar",
         {"ano": ano, "descricao": "Estacionar em local proibido", "pontuacao": 4, "placa": "XYZ9K88"}),
        (None, "detran/multas/lancar",
         {"ano": ano, "descricao": "Dirigir sem cinto", "pontuacao": 6, "placa": "DEF4G56"}),
        (None, "detran/multas/lancar",
         {"ano": ano, "descricao": "Ultrapassagem proibida", "pontuacao": 7, "placa": "GHI7J89"}),
        (None, "detran/multas/lancar",
         {"ano": ano, "descricao": "Uso de celular", "pontuacao": 3, "placa": "GHI7J89"}),
        (None, "detran/multas/lancar",
         {"ano": ano, "descricao": "Pneu careca", "pontuacao": 2, "placa": "JKL0M12"}),
        ("Transferir ABC1D23 para Carla (333)", "detran/veiculos/transferir",
         {"placa": "ABC1D23", "cpf": "333"}),
        ("Lançar multa após transferência", "detran/multas/lancar",
         {"ano": ano, "descricao": "Avanço de sinal", "pontuacao": 7, "placa": "ABC1D23"}),
        (f"Veículos emplacados em {ano}", "detran/veiculos/listar_por_ano", {"ano": ano}),
        ("Multas do veículo ABC1D23 (com condutor)", "detran/multas/por_veiculo", {"placa": "ABC1D23"}),
        (f"Multas do condutor 111 em {ano}", "detran/multas/por_condutor", {"cpf": "111", "ano": ano}),
        (f"Multas lançadas em {ano}", "detran/multas/por_ano", {"ano": ano}),
        ("Top 5 condutores por pontuação", "detran/multas/ranking", {}),
    ]
    for titulo, topic, data in passos:
        if titulo:
            print(f"\n### {titulo}")
        print(f"-> {topic} {json.dumps(data, ensure_ascii=False)}")
        mostrar(call(topic, data))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--demo", action="store_true", help="executa um roteiro de testes")
    args = ap.parse_args()
    conectar()
    demo() if args.demo else menu()
    client.loop_stop()
