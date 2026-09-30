---
name: weather
description: Look up a location's weather forecast using a live web search. Invoke whenever the user asks about the weather, whether to expect rain, temperature, or what to wear or plan for outdoor activities.
allowed-tools:
  - WebSearch
  - WebFetch
---

# Weather

## When to run
The user asks about the weather, a forecast, temperature, rain or snow chances, or whether a day suits an outdoor plan.

## How to answer
1. **Fix the date first.** Note today's date from the environment. Resolve "today", "tomorrow", or a weekday into an explicit calendar date (e.g. "Wed, July 8, 2026"). Carry that date through every step.
2. **Resolve the location.** If the user named it, use it. If not, ask one short question instead of guessing.
3. **Search to find a source, not to get the answer.** Always start with `WebSearch`, even when you already know a forecast URL: the search is how you confirm the source is current. Run a web search like `<city> weather forecast <date>` to locate a forecast page. Do NOT report numbers from the search-result summary itself. Those summaries frequently merge the wrong day or location and are the top cause of a wrong forecast.
4. **Fetch the actual source.** `WebFetch` the forecast page (prefer a national weather service such as weather.gov, or a major provider). Read the high, low, sky, and precipitation for **the exact date you fixed in step 1**.
5. **Verify the date matches before you report.** The fetched forecast must state the day you were asked about. If the page shows a different date, or you cannot fetch a page whose date matches, do not report a number. Say what you tried and that you could not confirm a current forecast. A plausible-looking summary for the wrong day is worse than saying you could not confirm.
6. **Report** in two or three sentences: expected high and low, sky conditions, and precipitation chance, with the location and the explicit date. Then name the source you actually fetched.

## Hard rules

- **Report only numbers you read from a page you fetched for the matching date.** Never report figures that came only from a search-result summary, and never cite a source you did not open.
- If nothing usable can be fetched and date-matched, say so and name what you tried. Do not fill the gap with a guess.
- Temperatures in Fahrenheit first, Celsius in parentheses.
- Keep the reply short enough to read in Slack without expanding.
