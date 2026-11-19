# Add Rankine temperature conversions

Add `r` (Rankine) to the temperature units accepted by `unitconv`.

1. Convert Celsius to Rankine with `(c + 273.15) * 9 / 5`, and Rankine to Celsius with the inverse.
2. Convert Fahrenheit to Rankine by adding 459.67, and Rankine to Fahrenheit by subtracting 459.67.
3. Preserve the existing conversions, including identity conversions.
4. Add regression tests in `unitconv/tests/test_convert.py` for the new directions and Rankine identity.
5. `python3 -m unittest discover -s unitconv/tests -v` passes.
