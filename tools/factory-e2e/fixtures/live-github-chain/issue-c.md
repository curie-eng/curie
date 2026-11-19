# Add Newton scale temperature conversions

Add `n` (Newton scale) to the temperature units accepted by `unitconv`.

1. Convert Celsius to Newton with `c * 33 / 100`, and Newton to Celsius with the inverse.
2. Preserve the existing conversions, including identity conversions.
3. Add regression tests in `unitconv/tests/test_convert.py` for both new directions and Newton identity.
4. `python3 -m unittest discover -s unitconv/tests -v` passes.
