from typing import Union


class PaddingDecimalFormat:
    """
    A decimal formatter that pads the output to a minimum length with spaces.
    Equivalent to Java's PaddingDecimalFormat which extends DecimalFormat.
    """

    def __init__(self, pattern: str, minimum_length: int):
        """
        Creates a PaddingDecimalFormat using the given pattern and minimum length.

        :param pattern: The decimal format pattern (e.g., "0.0", "0.00")
        :param minimum_length: The minimum length of the formatted string
        """
        self.pattern = pattern
        self.minimum_length = minimum_length

        # Parse the pattern to determine decimal places
        if '.' in pattern:
            decimal_part = pattern.split('.')[1]
            self.decimal_places = len(decimal_part)
        else:
            self.decimal_places = 0

    def format(self, number: Union[float, int]) -> str:
        """
        Format a number with padding.

        :param number: The number to format (int or float)
        :return: The formatted and padded string
        """
        # Format the number according to the pattern
        if self.decimal_places > 0:
            formatted = f"{number:.{self.decimal_places}f}"
        else:
            formatted = str(int(number))

        # Pad with spaces on the left if needed
        return self._pad(formatted)

    def _pad(self, formatted: str) -> str:
        """
        Pad the formatted string to the minimum length.

        :param formatted: The formatted number string
        :return: The padded string
        """
        num_length = len(formatted)
        pad_length = self.minimum_length - num_length

        if pad_length > 0:
            return ' ' * pad_length + formatted
        else:
            return formatted
