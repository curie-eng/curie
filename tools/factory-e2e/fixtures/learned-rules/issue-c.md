# Add a Delisle temperature conversion

Add `de` (Delisle) to the temperature units accepted by `unitconv`.

1. Convert Celsius to Delisle with `(100 - c) * 3 / 2`, and Delisle to Celsius with `100 - de * 2 / 3`.
2. Convert between Delisle and every other temperature unit through Celsius.
3. Preserve every existing temperature and length conversion, including identity conversions.
4. Add regression tests in `unitconv/tests/test_convert.py` for the new conversions and Delisle identity, asserting each floating point result. Implement the conversion in `unitconv/convert.py`.
5. `python3 -m unittest discover -s unitconv/tests -v` passes.
