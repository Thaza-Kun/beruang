# Beruang

A finance manager written in rust for bears and people with money.
The name is a Malay wordplay for "beruang" (bears) and "berwang" (someone with money).

## Features
Currently only support appending to the csv file, `transactions.csv` with the following header:

```csv
date,details,account,category,participant,currency,total
```

The filename can be set using the `--file` flag.