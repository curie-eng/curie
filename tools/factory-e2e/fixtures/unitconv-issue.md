# Add Kelvin temperature conversions

Add `k` (Kelvin) to the temperature units accepted by `unitconv`.

1. Convert Celsius to Kelvin by adding 273.15, and Kelvin to Celsius by subtracting 273.15.
2. Convert Fahrenheit to Kelvin and Kelvin to Fahrenheit through Celsius.
3. Preserve the existing Celsius, Fahrenheit and length conversions, including identity conversions.
4. Add regression tests in `unitconv/tests/test_convert.py` for all four new conversion directions and Kelvin identity. Implement the conversion in `unitconv/convert.py`.
5. Run `python3 -m unittest discover -s unitconv/tests -v` and publish one pull request with both files changed.
