"""
Script para exportar listagem de sellers do marketplace VTEX para CSV
com identificação do tipo de integração via fulfillmentEndpoint
"""
import requests
import csv
import json
from datetime import datetime
from typing import List, Dict
import os


def extrair_integracao(fulfillment_endpoint: str) -> str:
    """
    Lê o fulfillmentEndpoint e retorna o tipo de integração.
    - contém 'vtexcommerce' → 'VTEX'
    - contém 'anymarket'    → 'Anymarket'
    - outros                → 'Outros'
    """
    if not fulfillment_endpoint:
        return "Outros"
    url = fulfillment_endpoint.lower()
    if "vtexcommerce" in url:
        return "VTEX"
    elif "anymarket" in url:
        return "Anymarket"
    return "Outros"


class VTEXSellersExporter:
    """Classe para exportar sellers da VTEX"""

    def __init__(self, account_name: str, app_key: str, app_token: str):
        self.account_name = account_name
        self.base_url = f"https://{account_name}.vtexcommercestable.com.br"
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-VTEX-API-AppKey": app_key,
            "X-VTEX-API-AppToken": app_token
        }

    def get_all_sellers(self) -> List[Dict]:
        all_sellers = []
        seen_ids = set()
        page_size = 100
        total_sellers = None

        print("Iniciando busca de sellers...")

        while True:
            endpoint = f"{self.base_url}/api/catalog_system/pvt/seller/list"

            try:
                print(f"Buscando sellers...")
                response = requests.get(endpoint, headers=self.headers, timeout=30)
                response.raise_for_status()

                data = response.json()

                if isinstance(data, dict):
                    sellers = data.get('items', [])
                    paging = data.get('paging', {})

                    if total_sellers is None:
                        total_sellers = paging.get('total', 0)
                        print(f"  Total de sellers na loja: {total_sellers}")

                    new_sellers_count = 0
                    if sellers:
                        for seller in sellers:
                            if isinstance(seller, dict):
                                seller_id = seller.get('SellerId') or seller.get('id') or seller.get('sellerId')
                                if seller_id and seller_id not in seen_ids:
                                    all_sellers.append(seller)
                                    seen_ids.add(seller_id)
                                    new_sellers_count += 1

                        print(f"  Encontrados {new_sellers_count} sellers novos (de {len(sellers)} retornados)")
                        print(f"  Progresso: {len(all_sellers)}/{total_sellers} sellers")

                    if (total_sellers and len(all_sellers) >= total_sellers) or new_sellers_count == 0:
                        break

                else:
                    all_sellers = data if isinstance(data, list) else []
                    break

            except requests.exceptions.RequestException as e:
                print(f"Erro ao buscar sellers: {e}")
                break

        print(f"\nTotal de sellers encontrados: {len(all_sellers)}")
        return all_sellers

    def get_seller_details(self, seller_id: str) -> Dict:
        endpoint = f"{self.base_url}/api/seller-register/pvt/sellers/{seller_id}"
        try:
            response = requests.get(endpoint, headers=self.headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar detalhes do seller {seller_id}: {e}")
            return {}

    def export_to_csv(self, sellers: List[Dict], filename: str = None):
        if not sellers:
            print("Nenhum seller para exportar")
            return

        if filename is None:
            filename = f"output/vtex_sellers.csv"

        fieldnames = [
            'id',
            'name',
            'integracao',          # ← coluna nova de integração
            'fulfillmentEndpoint',
            'email',
            'description',
            'isActive',
            'sellerType',
            'taxCode',
            'allowHybridPayments',
            'trustPolicy',
            'exchangeReturnPolicy',
            'deliveryPolicy',
            'catalogSystemEndpoint',
            'availableSalesChannels',
            'channel',
            'CSCIdentification'
        ]

        all_keys = set()
        for seller in sellers:
            if isinstance(seller, dict):
                all_keys.update(seller.keys())

        for key in all_keys:
            if key not in fieldnames:
                fieldnames.append(key)

        field_mapping = {
            'SellerId': 'id',
            'Name': 'name',
            'Email': 'email',
            'Description': 'description',
            'ExchangeReturnPolicy': 'exchangeReturnPolicy',
            'DeliveryPolicy': 'deliveryPolicy',
            'UseHybridPaymentOptions': 'useHybridPaymentOptions',
            'UserName': 'userName',
            'Password': 'password',
            'TaxCode': 'taxCode',
            'IsActive': 'isActive',
            'FulfillmentEndpoint': 'fulfillmentEndpoint',
            'CatalogSystemEndpoint': 'catalogSystemEndpoint'
        }

        try:
            with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()

                for seller in sellers:
                    if not isinstance(seller, dict):
                        continue

                    row = {}
                    for key, value in seller.items():
                        target_key = field_mapping.get(key, key)
                        if isinstance(value, (list, dict)):
                            row[target_key] = json.dumps(value, ensure_ascii=False)
                        else:
                            row[target_key] = value

                    if 'id' not in row and 'SellerId' in seller:
                        row['id'] = seller['SellerId']

                    # ── Extrai integração do fulfillmentEndpoint ──
                    endpoint_url = (
                        row.get('fulfillmentEndpoint')
                        or row.get('FulfillmentEndpoint')
                        or seller.get('fulfillmentEndpoint')
                        or seller.get('FulfillmentEndpoint')
                        or ""
                    )
                    row['integracao'] = extrair_integracao(endpoint_url)

                    writer.writerow(row)

            print(f"\n[OK] Arquivo CSV criado: {filename}")
            print(f"  Total de sellers exportados: {len(sellers)}")

        except Exception as e:
            print(f"Erro ao criar arquivo CSV: {e}")
            raise

    def export_sellers(self, output_filename: str = None, include_details: bool = False):
        print("=" * 60)
        print("VTEX Sellers Exporter")
        print("=" * 60)

        sellers = self.get_all_sellers()

        if include_details and sellers:
            print("\nBuscando detalhes adicionais dos sellers...")
            detailed_sellers = []
            for i, seller in enumerate(sellers, 1):
                seller_id = seller.get('id') or seller.get('sellerId')
                if seller_id:
                    print(f"  [{i}/{len(sellers)}] Seller: {seller_id}")
                    details = self.get_seller_details(seller_id)
                    detailed_sellers.append(details if details else seller)
                else:
                    detailed_sellers.append(seller)
            sellers = detailed_sellers

        self.export_to_csv(sellers, output_filename)

        print("\n" + "=" * 60)
        print("Exportação concluída!")
        print("=" * 60)


def main():
    account_name = os.getenv('VTEX_ACCOUNT_NAME')
    app_key      = os.getenv('VTEX_APP_KEY')
    app_token    = os.getenv('VTEX_APP_TOKEN')

    if not all([account_name, app_key, app_token]):
        print("ERRO: Credenciais VTEX não configuradas!")
        print("  VTEX_ACCOUNT_NAME=sua-conta")
        print("  VTEX_APP_KEY=sua-app-key")
        print("  VTEX_APP_TOKEN=seu-app-token")
        return

    exporter = VTEXSellersExporter(
        account_name=account_name,
        app_key=app_key,
        app_token=app_token
    )

    # include_details=True busca o fulfillmentEndpoint via seller-register (mais preciso)
    exporter.export_sellers(include_details=True)


if __name__ == "__main__":
    main()
