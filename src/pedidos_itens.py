import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from tqdm import tqdm
import time
import os

# ========== CONFIGURAÇÕES ==========
ACCOUNT = os.getenv("VTEX_ACCOUNT_NAME", "senffnet")
ENV = "vtexcommercestable"
APP_KEY = os.getenv("VTEX_APP_KEY")
APP_TOKEN = os.getenv("VTEX_APP_TOKEN")

if not APP_KEY or not APP_TOKEN:
    raise RuntimeError("VTEX_APP_KEY ou VTEX_APP_TOKEN não definidos")

OUTPUT_PATH = "output/pedidos_itens.csv"
PER_PAGE = 50

headers = {
    "X-VTEX-API-AppKey": APP_KEY,
    "X-VTEX-API-AppToken": APP_TOKEN,
    "Content-Type": "application/json"
}

# ========== FUNÇÕES DE DATA ==========
def agora_brasil():
    return datetime.now(timezone.utc).astimezone(
        timezone(timedelta(hours=-3))
    )

def converter_brasil(data_iso):
    if not data_iso:
        return None
    return (
        datetime.fromisoformat(data_iso.replace("Z", "+00:00"))
        .astimezone(timezone(timedelta(hours=-3)))
        .strftime("%Y-%m-%d %H:%M:%S")
    )

def gerar_intervalo():
    agora = agora_brasil()
    inicio = (agora - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    fim = agora.replace(
        hour=23, minute=59, second=59, microsecond=0
    )
    return inicio, fim

# ========== COLETA DE ITENS ==========
def coletar_itens():
    registros = []
    pagina = 1
    order_ids_processados = set()

    inicio, fim = gerar_intervalo()
    i_utc = inicio.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    f_utc = fim.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    while True:
        url = (
            f"https://{ACCOUNT}.{ENV}.com.br/api/oms/pvt/orders?"
            f"f_creationDate=creationDate:[{i_utc} TO {f_utc}]"
            f"&per_page={PER_PAGE}&page={pagina}"
        )

        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code != 200:
            print(f"❌ Erro página {pagina}: {r.status_code}")
            break

        pedidos = r.json().get("list", [])
        if not pedidos:
            print(f"✅ Página {pagina} vazia — encerrando.")
            break

        novos_na_pagina = 0

        for resumo in tqdm(pedidos, desc=f"Página {pagina}"):
            order_id = resumo.get("orderId")
            if not order_id:
                continue

            # 🔐 TRAVA DEFINITIVA (VTEX REPETE PEDIDOS)
            if order_id in order_ids_processados:
                continue

            order_ids_processados.add(order_id)
            novos_na_pagina += 1

            url_det = f"https://{ACCOUNT}.{ENV}.com.br/api/oms/pvt/orders/{order_id}"

            pedido = None
            for _ in range(3):
                try:
                    r_det = requests.get(url_det, headers=headers, timeout=30)
                    if r_det.status_code == 200:
                        pedido = r_det.json()
                        break
                except requests.exceptions.RequestException:
                    time.sleep(2)

            if not pedido:
                continue

            creation_date_br = converter_brasil(pedido.get("creationDate"))

            for item in pedido.get("items", []):
                registros.append({
                    "creationDate": creation_date_br,
                    "orderId": order_id,
                    "additionalInfo_categories": item.get("additionalInfo", {}).get("categories"),
                    "name": item.get("name"),
                    "price": item.get("price"),
                    "listPrice": item.get("listPrice"),
                    "quantity": item.get("quantity"),
                    "productId": item.get("productId"),
                    "seller": item.get("seller"),
                    "data_extracao": agora_brasil().strftime("%Y-%m-%d %H:%M:%S")
                })

        # 🚨 SE NÃO VEIO NENHUM PEDIDO NOVO → PARA
        if novos_na_pagina == 0:
            print(f"✅ Nenhum pedido novo na página {pagina}. Encerrando.")
            break

        pagina += 1
        time.sleep(0.4)

    return registros

# ========== MAIN ==========
def main():
    os.makedirs("output", exist_ok=True)

    dados = coletar_itens()
    df = pd.DataFrame(dados)

    colunas_finais = [
        "creationDate",
        "orderId",
        "additionalInfo_categories",
        "name",
        "price",
        "listPrice",
        "quantity",
        "productId",
        "seller",
        "data_extracao"
    ]

    df = df[colunas_finais] if not df.empty else pd.DataFrame(columns=colunas_finais)
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    print(f"✅ CSV de itens gerado: {OUTPUT_PATH} ({len(df)} linhas)")

if __name__ == "__main__":
    main()
