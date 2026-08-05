import pandas as pd
import os
import logging
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import psycopg
import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="gold")
def gold(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🚀 Iniciando pipeline ETL Silver -> Gold...")

    KEY_VAULT_URL = os.environ.get("KEY_VAULT_URL", "https://kvxboxone.vault.azure.net/")

    credential = DefaultAzureCredential()
    vault_client = SecretClient(vault_url = KEY_VAULT_URL, credential = credential)

    account_name = vault_client.get_secret("storage-account-name").value
    account_key = vault_client.get_secret("storage-account-key").value

    storage_options = {
        "account_name": account_name,
        "account_key": account_key
    }

    try:
        df = pd.read_parquet(
            "abfs://silver@lakexboxone.dfs.core.windows.net/xbox_one_games.parquet",
            storage_options = storage_options
        )

        logging.info("✅ Arquivo do data lake lido com sucesso!")
    except:
        logging.error("❌ Leitura do arquivo no data lake falhou!")

        return func.HttpResponse(
            "❌ Erro no processamento!",
            status_code = 500
        )

    supabase_project = vault_client.get_secret("supabase-project-ref").value
    supabase_db_password = vault_client.get_secret("supabase-db-password").value 

    cols_formatted = ", ".join([f'"{col}"' for col in df.columns])

    # Atualiza todas as colunas em caso de conflito, exceto a chave primária
    update_cols = [col for col in df.columns if col not in ["id", "created_at"]]
    set_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])

    with psycopg.connect(
        f"postgresql://postgres:{supabase_db_password}@db.{supabase_project}.supabase.co:5432/postgres"
    ) as connection:
        with connection.cursor() as cur:
            cur.execute(
                "CREATE TEMP TABLE temp_games (LIKE xbox_one.games INCLUDING DEFAULTS) ON COMMIT DROP;"
            )

            with cur.copy(f"COPY temp_games ({cols_formatted}) FROM STDIN") as copy:
                for row in df.itertuples(index=False):
                    copy.write_row(row)

            cur.execute(f"""--sql
                INSERT INTO xbox_one.games ({cols_formatted})
                SELECT {cols_formatted}
                FROM temp_games
                ON CONFLICT (id) DO UPDATE SET
                {set_clause};
            """)

    return func.HttpResponse(
         "Dados salvo no banco com sucesso!",
         status_code = 200
    )