from cryptography.fernet import Fernet

class Cryptor:
    def __init__(self, key):
        self.key = key
        self.fernet = Fernet(self.key)

    def encrypt(self, data):
        encrypted_data = {}

        for key, value in data.items():
            encrypted_data[key] = self.fernet.encrypt(str(value).encode()).decode()

        return encrypted_data

    def decrypt(self, data):
        decrypted_data = {}

        for key, value in data.items():
            decrypted_data[key] = self.fernet.decrypt(value.encode()).decode()

        return decrypted_data
