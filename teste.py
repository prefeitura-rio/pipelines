from pipelines.utils.utils import(
    get_vault_secret,
)

try:
    print(get_vault_secret("estoque_tpc"))
except:
    pass