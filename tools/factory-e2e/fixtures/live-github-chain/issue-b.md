# Add Delisle temperature conversions

Add `de` (Delisle) to the temperature units accepted by `unitconv`.

1. Convert Celsius to Delisle with `(100 - c) * 3 / 2`, and Delisle to Celsius with the inverse.
2. Preserve the existing conversions, including identity conversions.
3. Add regression tests in `unitconv/tests/test_convert.py` for both new directions and Delisle identity.
4. `python3 -m unittest discover -s unitconv/tests -v` passes.
