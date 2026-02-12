"""
Script para exportar listagem de sellers do marketplace VTEX para CSV
"""
import requests
import csv
import json
from datetime import datetime
from typing import List, Dict
import os


class VTEXSellersExporter:
    """Classe para exportar sellers da VTEX"""
    
    def __init__(self, account_name: str, app_key: str, app_token: str):
        """
        Inicializa o exportador de sellers
        
        Args:
            account_name: Nome da conta VTEX (ex: 'minhaloja')
            app_key: App Key da VTEX
            app_token: App Token da VTEX
        """
        self.account_name = account_name
        self.base_url = f"https://{account_name}.vtexcommercestable.com.br"
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-VTEX-API-AppKey": app_key,
            "X-VTEX-API-AppToken": app_token
        }
    
    def get_all_sellers(self) -> List[Dict]:
        """
        Busca todos os sellers do marketplace (com paginação e deduplicação)
        
        Returns:
            Lista de dicionários com dados dos sellers
        """
        all_sellers = []
        seen_ids = set()  # Para evitar duplicatas
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
                                # VTEX Catalog API uses 'SellerId' or 'SellerId' (PascalCase)
                                # Seller Register API uses 'id' or 'sellerId' (camelCase)
                                seller_id = seller.get('SellerId') or seller.get('id') or seller.get('sellerId')
                                if seller_id and seller_id not in seen_ids:
                                    all_sellers.append(seller)
                                    seen_ids.add(seller_id)
                                    new_sellers_count += 1
                        
                        print(f"  Encontrados {new_sellers_count} sellers novos (de {len(sellers)} retornados)")
                        print(f"  Progresso: {len(all_sellers)}/{total_sellers} sellers")
                    
                    # Conclui se já pegamos todos ou se não vieram novos
                    if (total_sellers and len(all_sellers) >= total_sellers) or new_sellers_count == 0:
                        break
                    
                else:
                    all_sellers = data if isinstance(data, list) else []
                    break
                    
            except requests.exceptions.RequestException as e:
                print(f"Erro ao buscar sellers: {e}")
                break
        
        print(f"\nTotal de sellers registrados encontrados: {len(all_sellers)}")
        return all_sellers
    
    def get_seller_details(self, seller_id: str) -> Dict:
        """
        Busca detalhes de um seller específico
        
        Args:
            seller_id: ID do seller
            
        Returns:
            Dicionário com detalhes do seller
        """
        endpoint = f"{self.base_url}/api/seller-register/pvt/sellers/{seller_id}"
        
        try:
            response = requests.get(endpoint, headers=self.headers, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar detalhes do seller {seller_id}: {e}")
            return {}
    
    def export_to_csv(self, sellers: List[Dict], filename: str = None):
        """
        Exporta lista de sellers para arquivo CSV
        
        Args:
            sellers: Lista de sellers
            filename: Nome do arquivo (opcional)
        """
        if not sellers:
            print("Nenhum seller para exportar")
            return
        
        # Gera nome do arquivo com timestamp se não fornecido
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"output/vtex_sellers.csv"
        
        # Define os campos do CSV baseado nas chaves disponíveis
        # Campos comuns da API de sellers VTEX
        fieldnames = [
            'id',
            'name',
            'email',
            'description',
            'exchangeReturnPolicy',
            'deliveryPolicy',
            'useHybridPaymentOptions',
            'userName',
            'password',
            'SonarConfiguration',
            'taxCode',
            'isActive',
            'fulfillmentEndpoint',
            'catalogSystemEndpoint',
            'allowHybridPayments',
            'sellerType',
            'availableSalesChannels',
            'trustPolicy',
            'channel',
            'sellerId',
            'CSCIdentification'
        ]
        
        # Adiciona campos adicionais que possam existir
        all_keys = set()
        for seller in sellers:
            # Ignora itens que não são dicionários
            if isinstance(seller, dict):
                all_keys.update(seller.keys())
        
        # Adiciona campos extras que não estão na lista padrão
        for key in all_keys:
            if key not in fieldnames:
                fieldnames.append(key)
        
        # Mapeamento de campos PascalCase (Catalog API) para camelCase (Script/Register API)
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
                    # Ignora itens que não são dicionários
                    if not isinstance(seller, dict):
                        continue
                        
                    # Prepara a linha normalizando os nomes dos campos
                    row = {}
                    for key, value in seller.items():
                        # Mapeia a chave se necessário
                        target_key = field_mapping.get(key, key)
                        
                        # Converte campos complexos (listas, dicts) para JSON string
                        if isinstance(value, (list, dict)):
                            row[target_key] = json.dumps(value, ensure_ascii=False)
                        else:
                            row[target_key] = value
                    
                    # Garante que 'id' esteja preenchido se 'SellerId' existir mas 'id' não
                    if 'id' not in row and 'SellerId' in seller:
                        row['id'] = seller['SellerId']
                    
                    writer.writerow(row)
            
            print(f"\n[OK] Arquivo CSV criado com sucesso: {filename}")
            print(f"  Total de sellers exportados: {len(sellers)}")
            
        except Exception as e:
            print(f"Erro ao criar arquivo CSV: {e}")
            raise
    
    def export_sellers(self, output_filename: str = None, include_details: bool = False):
        """
        Método principal para exportar sellers
        
        Args:
            output_filename: Nome do arquivo de saída (opcional)
            include_details: Se True, busca detalhes adicionais de cada seller
        """
        print("=" * 60)
        print("VTEX Sellers Exporter")
        print("=" * 60)
        
        # Busca lista de sellers
        sellers = self.get_all_sellers()
        
        # Se solicitado, busca detalhes adicionais
        if include_details and sellers:
            print("\nBuscando detalhes adicionais dos sellers...")
            detailed_sellers = []
            for i, seller in enumerate(sellers, 1):
                seller_id = seller.get('id') or seller.get('sellerId')
                if seller_id:
                    print(f"  [{i}/{len(sellers)}] Buscando detalhes do seller: {seller_id}")
                    details = self.get_seller_details(seller_id)
                    if details:
                        detailed_sellers.append(details)
                    else:
                        detailed_sellers.append(seller)
                else:
                    detailed_sellers.append(seller)
            sellers = detailed_sellers
        
        # Exporta para CSV
        self.export_to_csv(sellers, output_filename)
        
        print("\n" + "=" * 60)
        print("Exportação concluída!")
        print("=" * 60)


def main():
    """Função principal"""
    
    # Carrega credenciais das variáveis de ambiente
    account_name = os.getenv('VTEX_ACCOUNT_NAME')
    app_key = os.getenv('VTEX_APP_KEY')
    app_token = os.getenv('VTEX_APP_TOKEN')
    
    # Valida credenciais
    if not all([account_name, app_key, app_token]):
        print("ERRO: Credenciais VTEX não configuradas!")
        print("  VTEX_ACCOUNT_NAME=sua-conta")
        print("  VTEX_APP_KEY=sua-app-key")
        print("  VTEX_APP_TOKEN=seu-app-token")
        return
    
    # Cria instância do exportador
    exporter = VTEXSellersExporter(
        account_name=account_name,
        app_key=app_key,
        app_token=app_token
    )
    
    # Executa exportação
    # Para incluir detalhes adicionais, use: include_details=True
    exporter.export_sellers(include_details=False)


if __name__ == "__main__":
    main()
