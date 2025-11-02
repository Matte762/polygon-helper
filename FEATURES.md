New proposed structure:
polygon_helper/
  client.py            # shared infra
  stocks/              # equity vertical
    __init__.py
    api.py             # public helpers: price series, ref data, etc.
    normalize.py       # JSON→DataFrame (stock-specific)
    errors.py          # domain errors (optional)
  options/             # options vertical
    __init__.py
    api.py             # public helpers: contract series, chains, etc.
    normalize.py       # JSON→DataFrame (option-specific)
    errors.py
  cli.py               # thin façade: routes to stocks.* or options.* “main”
  __main__.py