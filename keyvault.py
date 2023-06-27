from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import logging

class KeyVault(object):

    def __init__(self,vault_name):
        self.vault_name = vault_name
        self.KVUri = 'https://'+ vault_name + '.vault.azure.net/'  
        self.credential = DefaultAzureCredential(additionally_allowed_tenants=['*'])
        self.client = SecretClient(vault_url=self.KVUri, credential=self.credential)
        #self.logger = logging.getLogger('func_get_quickbooks_object.keyvault')

    def get_secret(self, secret_name):

        try:
            secret = self.client.get_secret(secret_name)
            return secret.value
        except Exception as e:
            raise Exception(f"Failed to retrieve secret '{secret_name}' from Azure Key Vault: {e}")

    def set_secret(self, secret_name, secret_value):
        try:
            current_secret = self.client.get_secret(secret_name)
            if current_secret.value == secret_value:
                self.logger.debug('Secret matches what is already in key vault. No updated required')
            else:
                self.client.set_secret(secret_name, secret_value)
                self.logger.debug('Secret has been successfully updated in Key Vault')
        except Exception as e:
            raise Exception(f"Failed to set secret '{secret_name}' in Azure Key Vault: {e}")

    def list_all_secrets(self):
        secrets = self.client.list_properties_of_secrets()

        return secrets


# my_vault = KeyVault('akv-apipoc-dev')
# print(my_vault.get_secret('mysecret'))