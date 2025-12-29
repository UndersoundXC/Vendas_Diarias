import os
import pandas as pd
from datetime import datetime

OUTPUT_PATH = "output/pedidos.csv"

def main():
    # cria a pasta output se não existir
    os.makedirs("output", exist_ok=True)

    # dados de teste (vamos trocar depois pelos pedidos reais)
    dados = [
        {
            "pedido_id": "TESTE-001",
            "produto": "Produto Exemplo",
            "quantidade": 1,
            "valor": 99.90,
            "data_execucao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    ]

    df = pd.DataFrame(dados)

    # grava CSV
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    print("CSV gerado com sucesso:", OUTPUT_PATH)

if __name__ == "__main__":
    main()
