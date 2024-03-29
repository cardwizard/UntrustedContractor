from cryptography.fernet import Fernet


class Cryptor:
    def __init__(self, key):
        self.key = key
        self.fernet = Fernet(self.key)

    def encrypt(self, data, skip_keys):
        encrypted_data = {}

        for key, value in data.items():
            if key in skip_keys:
                encrypted_data[key] = value
                continue
            encrypted_data[key] = self.fernet.encrypt(str(value).encode()).decode()

        return encrypted_data

    def decrypt(self, data):
        decrypted_data = {}

        for key, value in data.items():
            try:
                decrypted_data[key] = self.fernet.decrypt(value.encode()).decode()
            except:
                decrypted_data[key] = value

        return decrypted_data
