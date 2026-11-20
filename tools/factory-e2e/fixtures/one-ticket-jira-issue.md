# Add an optional rounding of converted values to unitconv

Add an optional rounding of the converted value to `unitconv`, so a caller can ask `convert(value, src, dst)` for a rounded result instead of the raw float. Which rounding mode the option uses, and how many decimal places it rounds to when the caller names none, are not decided here.

1. Add the rounding as an optional keyword argument of `convert` in `unitconv/convert.py`; a call that does not pass it returns exactly what it returns today.
2. Round the result of every conversion family the same way: length and temperature, identity conversions included.
3. Preserve every existing conversion, including identity conversions and the `UnknownUnit` and mixed-family errors.
4. Add regression tests in `unitconv/tests/test_convert.py` for the rounding mode, the default precision, an explicit precision, and a call without the option.
5. `python3 -m unittest discover -s unitconv/tests -v` passes.
