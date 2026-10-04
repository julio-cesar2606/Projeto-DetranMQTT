import json
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
import paho.mqtt.client as mqtt

SERVICE = "multas"
BROKER = os.getenv("MQTT_HOST", "broker")
PORT = int(os.getenv("MQTT_PORT", "1883"))
REPLY_TOPIC = f"detran/replies/{SERVICE}-{uuid.uuid4().hex[:8]}"

HANDLERS = {}
pending = {}
executor = ThreadPoolExecutor(max_workers=8)
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id=f"{SERVICE}-{uuid.uuid4().hex[:6]}")

class ServiceError(Exception):
    """Erro de negócio devolvido ao solicitante."""

def handler(name):
    def deco(fn):
        HANDLERS[name] = fn
        return fn
    return deco


def call(topic, data, timeout=5):
    corr_id = uuid.uuid4().hex
    slot = {"event": threading.Event(), "resp": None}
    pending[corr_id] = slot
    payload = {"corr_id": corr_id, "reply_to": REPLY_TOPIC, "data": data}
    client.publish(topic, json.dumps(payload), qos=1)
    try:
        if not slot["event"].wait(timeout):
            raise ServiceError(f"timeout aguardando resposta de {topic}")
    finally:
        pending.pop(corr_id, None)
    resp = slot["resp"]
    if not resp["ok"]:
        raise ServiceError(resp["error"])
    return resp["data"]


def _handle(msg):
    try:
        req = json.loads(msg.payload)
    except ValueError:
        return
    action = msg.topic.rsplit("/", 1)[-1]
    try:
        fn = HANDLERS.get(action)
        if fn is None:
            raise ServiceError(f"ação desconhecida: {action}")
        resp = {"ok": True, "data": fn(req.get("data") or {})}
    except ServiceError as e:
        resp = {"ok": False, "error": str(e)}
    except Exception as e:  # noqa: BLE001
        resp = {"ok": False, "error": f"erro interno: {e}"}
    resp["corr_id"] = req.get("corr_id")
    if req.get("reply_to"):
        client.publish(req["reply_to"], json.dumps(resp), qos=1)


def _on_connect(c, userdata, flags, reason_code, properties):
    c.subscribe([(f"detran/{SERVICE}/+", 1), (REPLY_TOPIC, 1)])
    print(f"[{SERVICE}] conectado ao broker; ouvindo detran/{SERVICE}/+", flush=True)


def _on_message(c, userdata, msg):
    if msg.topic == REPLY_TOPIC:
        try:
            resp = json.loads(msg.payload)
        except ValueError:
            return
        slot = pending.get(resp.get("corr_id"))
        if slot:
            slot["resp"] = resp
            slot["event"].set()
    else:
        executor.submit(_handle, msg)


def run():
    client.on_connect = _on_connect
    client.on_message = _on_message
    while True:
        try:
            client.connect(BROKER, PORT, keepalive=60)
            break
        except OSError:
            print(f"[{SERVICE}] broker indisponivel, tentando novamente", flush=True)
            time.sleep(2)
    client.loop_forever()


def _s(d, k):
    return str(d.get(k, "")).strip()


def _int(d, k):
    try:
        return int(d.get(k))
    except (TypeError, ValueError):
        raise ServiceError(f"campo '{k}' invalido")

multas = []
lock = threading.Lock()


def _placa(d):
    placa = _s(d, "placa").upper()
    if not placa:
        raise ServiceError("placa é obrigatória")
    return placa


def _com_condutor(lista):
    cache, saida = {}, []
    for m in lista:
        if m["cpf"] not in cache:
            cache[m["cpf"]] = call("detran/condutores/consultar", {"cpf": m["cpf"]})
        saida.append({**m, "condutor": cache[m["cpf"]]})
    return saida


@handler("lancar")
def lancar(d):
    ano, pontuacao = _int(d, "ano"), _int(d, "pontuacao")
    descricao, placa = _s(d, "descricao"), _placa(d)
    if not descricao or pontuacao <= 0:
        raise ServiceError("descricao e pontuacao (> 0) são obrigatórias")

    veiculo = call("detran/veiculos/consultar", {"placa": placa}) 
    
    with lock:
        multa = {"id": len(multas) + 1, "ano": ano, "descricao": descricao,"pontuacao": pontuacao, "placa": placa,"cpf": veiculo["cpf"]}  
        multas.append(multa)
        return multa


@handler("por_veiculo")
def por_veiculo(d):
    placa = _placa(d)
    ano = int(d["ano"]) if d.get("ano") else None
    with lock:
        lista = [m for m in multas
                 if m["placa"] == placa and (ano is None or m["ano"] == ano)]
    return _com_condutor(lista)


@handler("por_condutor")
def por_condutor(d):
    cpf, ano = _s(d, "cpf"), _int(d, "ano")
    with lock:
        return [m for m in multas if m["cpf"] == cpf and m["ano"] == ano]


@handler("por_ano")
def por_ano(d):
    ano = _int(d, "ano")
    with lock:
        return [m for m in multas if m["ano"] == ano]


@handler("ranking")
def ranking(d):
    totais = {}
    with lock:
        for m in multas:
            totais[m["cpf"]] = totais.get(m["cpf"], 0) + m["pontuacao"]
    top = sorted(totais.items(), key=lambda kv: kv[1], reverse=True)[:5]
    saida = []
    for cpf, pontos in top:
        cond = call("detran/condutores/consultar", {"cpf": cpf})
        saida.append({"cpf": cpf, "nome": cond["nome"], "pontuacao_total": pontos})
    return saida


if __name__ == "__main__":
    run()
