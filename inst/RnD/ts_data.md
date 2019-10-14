# What could/should a ts_data record look like?

## Regular
```json
{
  "ts_frequency": 12,
  "ts_start": "2019-10-14",
  "ts_data": [1, 2, 3, 4],
  "ts_regular": true
}
```
* Saves storage space by omitting most time stamps
* Modeled closely after R's `stats::ts` representation
* Most "official statistics" should be regular

Alternatively `ts_frequency` could be stored in a separate column,
but it is not needed for any other purpose.

## Irregular
```json
{
  "ts_regular": false,
  "ts_times": ["2019-10-14", "2019-10-15", "2019-10-16"],
  "ts_data": [2, 3, 4]
}
```