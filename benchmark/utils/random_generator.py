import random
import base64


class RandomGenerator:

    _random = random.Random()

    @staticmethod
    def get_random_string() -> str:
        buffer = RandomGenerator._random.randbytes(5)
        return base64.urlsafe_b64encode(buffer).decode('ascii').rstrip('=')
