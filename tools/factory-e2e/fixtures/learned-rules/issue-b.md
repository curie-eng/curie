# Add Rankine temperature conversions

Add `r` (Rankine) to the temperature units accepted by `unitconv`.

1. Convert Fahrenheit to Rankine by adding 459.67, and Rankine to Fahrenheit by subtracting 459.67.
2. Convert Celsius and Kelvin to Rankine, and Rankine to Celsius and Kelvin, through Fahrenheit or Kelvin.
3. Preserve every existing temperature and length conversion, including identity conversions.
4. Add regression tests in `unitconv/tests/test_convert.py` for every new conversion direction and Rankine identity, asserting each floating point result. Implement the conversion in `unitconv/convert.py`.
5. `python3 -m unittest discover -s unitconv/tests -v` passes.
