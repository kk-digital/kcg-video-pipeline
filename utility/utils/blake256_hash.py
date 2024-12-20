from Crypto.Hash import BLAKE2s
import aiofiles

class Blake256:
    """
    This class defines the standard hash for images using BLAKE-256.
    """

    def __init__(self, file_path: str = None, data: bytes = None):
        """
        Initialize the hash object from a file or data.
        """
        self.hash = BLAKE2s.new(digest_bits=256)
        if file_path:
            with open(file_path, 'rb') as file:
                while chunk := file.read(8192):
                    self.hash.update(chunk)
        elif data:
            self.hash.update(data)
        else:
            raise ValueError("Either file_path or data must be provided.")

    @classmethod
    async def async_init(cls, file_path: str):
        """
        Asynchronously create a Hash object from a file.
        """
        instance = cls.__new__(cls)
        instance.hash = BLAKE2s.new(digest_bits=256)
        async with aiofiles.open(file_path, 'rb') as f:
            while True:
                chunk = await f.read(8192)
                if not chunk:
                    break
                instance.hash.update(chunk)
        return instance

    def hexdigest(self) -> str:
        """
        Return the hexadecimal representation of the hash.
        """
        return self.hash.hexdigest()

    def digest(self) -> bytes:
        """
        Return the binary representation of the hash.
        """
        return self.hash.digest()
